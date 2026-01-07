# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
from typing import List, Optional

import pytz
from discord import Embed
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_discord_embed, send_discord_webhook
from pipelines.utils.tasks import get_bigquery_project_from_environment


@task
def get_data(dataset_name: str, table_name: str, environment: str):
    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment="prod")

    current_time = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    # Fora de horas úteis, queremos conferir de 1 em 1 hora
    # Damos leniência de 15 min antes ou depois
    start_time = current_time.replace(hour=7, minute=45)
    end_time = current_time.replace(hour=19, minute=15)
    IS_WORKDAY = current_time >= start_time and current_time <= end_time

    interval = "30 MINUTE" if IS_WORKDAY else "1 HOUR"
    # Quantidade de intervalos de 30 minutos em horas úteis de 7 dias
    # 7 (dias) × 9 (horas) × 2 (meia-horas)
    denom = (7 * 9 * 2) if IS_WORKDAY else (7 * 9)

    def get_query(interval: str = "30 MINUTE", denom: Optional[int] = 1):
        return f"""
SELECT
    tipo_evento,
    resultado,
    JSON_VALUE(parametros_adicionais.params[0].page) as pagina,
    (COUNT(*) / ({denom})) AS qtd
FROM `{project_name}.{dataset_name}.{table_name}`
WHERE created_at >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL {interval})
GROUP BY 1, 2, 3
        """

    QUERY_RECENT = get_query(interval=interval)
    rows_recent = [row.values() for row in client.query(QUERY_RECENT).result()]
    log(f"Query for `tipo_evento`, `resultado` ({interval}) returned {len(rows_recent)} row(s)")

    QUERY_7D = get_query(interval="7 DAY", denom=denom)
    rows_7d = [row.values() for row in client.query(QUERY_7D).result()]
    log(f"Query for `tipo_evento`, `resultado` (7 DAY) returned {len(rows_7d)} row(s)")

    cpf_count = [
        # Trata casos de [] e [None] -> 0
        (row.values() or [0])[0] or 0
        for row in client.query(
            f"""
SELECT
    count(distinct autor_cpf)
FROM `{project_name}.{dataset_name}.{table_name}`
WHERE created_at >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL {interval})
        """
        ).result()
    ][0]

    return {
        "unique_users": cpf_count,
        "30min": rows_recent,  # Listas de tuplas (evento, quantidade)
        "7d": rows_7d,
    }


@task
def send_report(data, environment: str):
    current_time = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    # Só queremos avisos em horas úteis (8:00–19:00)
    # Damos leniência de 15 min antes ou depois
    start_time = current_time.replace(hour=7, minute=45)
    end_time = current_time.replace(hour=19, minute=15)
    DURING_WORK_HOURS = current_time >= start_time and current_time <= end_time

    # Seg=0, Ter=1, Qua=2, Qui=3, Sex=4, Sab=5, Dom=6
    DURING_WORK_DAYS = current_time.weekday() < 5

    IS_WORKDAY = DURING_WORK_HOURS and DURING_WORK_DAYS

    INTERVAL = "30min" if IS_WORKDAY else "1h"

    # Fora de horas úteis, queremos só executar de 1 em 1 hora
    if not IS_WORKDAY:
        # Novamente leniência de 15 min antes ou depois
        if current_time.minute >= 15 and current_time.minute <= 45:
            log(
                "Flow is configured to only run in 1 hour intervals; "
                f"it is currently minute {current_time.minute} (waits for <15, >45)"
            )
            return

    unique_users = data["unique_users"]
    data_recent = data["30min"]
    data_7d = data["7d"]

    log(f"Últimos {INTERVAL}:\n{data_recent}")
    log(f"Últimos 7 dias (média):\n{data_7d}")

    warnings = []
    sections = {"access": dict(), "use": dict(), "others": dict()}
    # Para cada evento recente:
    for event in data_recent:
        evt_type: str = event[0]
        status: str = event[1]
        page: int = int(event[2] or 0)
        amount: float = event[3]
        if amount <= 0:
            continue
        # Pega média semanal do evento
        average = next(
            (
                average
                for (endpoint_7d, status_7d, _, average) in data_7d
                if endpoint_7d == evt_type and status_7d == status
            ),
            0,
        )
        section = "others"
        if (
            evt_type.startswith("Login")
            or evt_type.startswith("Termos")
            or evt_type.startswith("Dados do usuário")
        ):
            section = "access"
        elif evt_type.startswith("Busca") or evt_type.startswith("Consulta"):
            section = "use"

        if status == "500" and not evt_type.startswith("("):
            warnings.append(f"🚨 HTTP 500 em {evt_type}")

        if evt_type not in sections[section]:
            sections[section][evt_type] = dict()
        if page not in sections[section][evt_type]:
            sections[section][evt_type][page] = []

        s = "" if amount < 2 else "s"
        amount_str = (
            f"[p.{page}] **{amount:.0f}** ocorrência{s}"
            if page > 0
            else f"**{amount:.0f}** ocorrência{s} (média: {average:.2f})"
        )
        sections[section][evt_type][page].append((status, amount_str))

    # Além disso:
    # - Verifica se só temos logins/buscas/etc, mas não consultas
    types: List[str] = [
        evt_type.lower() for (evt_type, status, _, _) in data_recent if status == "200"
    ]
    actual_usage_count = len([t for t in types if t.startswith("consulta")])
    if actual_usage_count <= 0:
        emoji = "🚨" if IS_WORKDAY else "⚠️"
        warnings.append(f"{emoji} Nenhuma consulta no último intervalo de {INTERVAL}!")

    ####################################
    # Notificações
    ####################################

    HTTP_STATUS = {
        "400": "Requisição mal formatada",
        "401": "Não autorizado",
        "403": "Permissão negada",
        "404": "Não encontrado",
        "429": "Excesso de acessos",
        "500": "⚠️ Erro interno",
    }

    def create_section(title, events):
        embed = None
        if IS_WORKDAY:
            embed = Embed(title=title, color=0xDBDBE5, timestamp=current_time)
        else:
            embed = Embed(title=f"💤 {title}", color=0x95A7C9, timestamp=current_time)

        event_types = sorted(list(events.keys()))
        for evt_type in event_types:
            pages = sorted(list(events[evt_type].keys()))
            outstr = ""
            for page in pages:
                for status_code, text in events[evt_type][page]:
                    # O Discord faz um .strip() em textos, então pra fazer
                    # espaçamento customizado é preciso burlar com caracteres
                    # esquisitos:
                    # -> U+034F COMBINING GRAPHEME JOINER
                    # -> U+2003 EM SPACE
                    outstr += "\u034f \u2003 "
                    if status_code == "200":
                        outstr += text
                    elif status_code in HTTP_STATUS.keys():
                        outstr += text + f" [**{status_code}**: {HTTP_STATUS[status_code]}]"
                    else:
                        outstr += text + f" [**{status_code}**: ❗ Status inesperado]"
                    outstr += "\n"
            log(f"'📄 {evt_type}'; '{outstr}'")
            embed.add_field(name=f"📄 {evt_type}", value=outstr, inline=False)

        interval_text = "meia" if IS_WORKDAY else "1"
        embed.set_footer(text=f"Última {interval_text} hora (vs. média semanal)")
        return embed

    all_embeds = []
    if len(sections["access"]):
        all_embeds.append(create_section("Acessos", sections["access"]))
    if len(sections["use"]):
        all_embeds.append(create_section("Uso", sections["use"]))
    if len(sections["others"]):
        all_embeds.append(create_section("Outros", sections["others"]))

    ####################################
    # Informações
    ####################################
    usercount = unique_users if unique_users > 0 else "Nenhum"
    s = "" if unique_users < 2 else "s"
    ou = "ou" if unique_users < 2 else "aram"
    meia = "meia" if IS_WORKDAY else "1"
    usage_report_str = f"{usercount} pessoa{s} us{ou} o HCI na última {meia} hora"
    log(usage_report_str)
    asyncio.run(
        send_discord_webhook(
            text_content=usage_report_str,
            username="Monitoramento HCI",
            monitor_slug="hci_status",
        )
    )

    if len(all_embeds) > 0:
        asyncio.run(
            send_discord_embed(
                contents=all_embeds,
                username="Monitoramento HCI",
                monitor_slug="hci_status",
            )
        )

    ####################################
    # Alertas
    ####################################
    ROLE_ID = "1224334248345862164"
    ROLE_MENTION = f"<@&{ROLE_ID}>" if environment == "prod" else "@(dev)"
    if len(warnings) > 0:
        log(f"Sending warnings: {warnings}", level="warning")
        message = ""
        if IS_WORKDAY:
            message = f"**Alertas** ({ROLE_MENTION}):\n"
        message += "\n".join(warnings)
        asyncio.run(
            send_discord_webhook(
                text_content=message,
                username="Monitoramento HCI",
                monitor_slug="hci_status",
            )
        )
