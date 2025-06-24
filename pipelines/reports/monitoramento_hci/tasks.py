# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
from typing import Optional

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

    def get_query(interval: str = "30 MINUTE", denom: Optional[int] = 1):
        return f"""
SELECT tipo_evento, resultado, (COUNT(*) / ({denom})) AS cnt
FROM `{project_name}.{dataset_name}.{table_name}`
WHERE created_at >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL {interval})
GROUP BY 1, 2
        """

    QUERY_30M = get_query(interval="30 MINUTE")
    rows_30m = [row.values() for row in client.query(QUERY_30M).result()]
    log(f"Query for tipo_evento, resultado (30 min) returned {len(rows_30m)} row(s)")

    # Quantidade de intervalos de 30 minutos em horas úteis de 7 dias
    # 7 (dias) × 8 (horas) × 2 (meia-horas)
    denom = 7 * 8 * 2
    QUERY_7D = get_query(interval="7 DAY", denom=denom)
    rows_7d = [row.values() for row in client.query(QUERY_7D).result()]
    log(f"Query for tipo_evento, resultado (7 d) returned {len(rows_7d)} row(s)")
    # Retorna duas listas de tuplas (evento, quantidade)
    return (rows_30m, rows_7d)


@task
def send_report(data):
    data_30m = data[0]
    data_7d = data[1]

    log(f"Últimos 30 min:\n{data_30m}")
    log(f"Últimos 7 dias (média):\n{data_7d}")

    warnings = []
    sections = {"access": dict(), "use": dict(), "others": dict()}
    # Para cada evento recente:
    for event in data_30m:
        evt_type: str = event[0]
        status: str = event[1]
        amount: float = event[2]
        if amount <= 0:
            continue
        if "desenvolvimento" in evt_type.lower():
            continue
        # Pega média semanal do evento
        average = next(
            (
                average
                for (endpoint_7d, status_7d, average) in data_7d
                if endpoint_7d == evt_type and status_7d == status
            ),
            0,
        )
        section = "others"
        if evt_type.startswith("Login") or evt_type.startswith("Termos"):
            section = "access"
        elif evt_type.startswith("Busca") or evt_type.startswith("Consulta"):
            section = "use"

        s = "" if amount < 2 else "s"
        if status == "500" and not evt_type.startswith("("):
            warnings.append(f"🚨 HTTP 500 em {evt_type}")

        if evt_type not in sections[section]:
            sections[section][evt_type] = []

        sections[section][evt_type].append(
            (status, f"**{amount:.0f}** ocorrência{s} (média: {average:.2f})")
        )

    # Além disso:
    # - Verifica se só temos logins/buscas/etc, mas não consultas
    types = [evt_type.lower() for (evt_type, status, _) in data_30m if status == "200"]
    actual_usage_count = len([t for t in types if t.startswith("consulta")])
    if actual_usage_count <= 0:
        warnings.append(f"🚨 Nenhuma consulta na última meia hora!")

    # TODO: Testar requisição à API diretamente

    ####################################
    ## Notificações
    ####################################

    HTTP_STATUS = {
        "400": "Requisição mal formatada",
        "401": "Não autorizado",
        "403": "Permissão negada",
        "404": "Não encontrado",
        "500": "⚠️ Erro interno",
    }

    def create_section(title, obj):
        embed = Embed(
            title=title,
            color=0xDBDBE5,
            timestamp=datetime.now(tz=pytz.timezone("America/Sao_Paulo")),
        )

        keys = list(obj.keys())
        keys.sort()
        for key in keys:
            outstr = ""
            for status_code, text in obj[key]:
                outstr += "\u00b7 \u2003 "  # middle dot (U+00B7) + em space (U+2003)
                if status_code == "200":
                    outstr += text
                elif status_code in HTTP_STATUS.keys():
                    outstr += text + f" [**{status_code}**: {HTTP_STATUS[status_code]}]"
                else:
                    outstr += text + f" [**{status_code}**: ❓]"
                outstr += "\n"
            embed.add_field(name=f"📄 {key}", value=outstr, inline=False)

        embed.set_footer(text="Última meia hora (vs. média semanal)")
        return embed

    all_embeds = []
    if len(sections["access"]):
        all_embeds.append(create_section("Acessos", sections["access"]))
    if len(sections["use"]):
        all_embeds.append(create_section("Uso", sections["use"]))
    if len(sections["others"]):
        all_embeds.append(create_section("Outros", sections["others"]))

    if len(all_embeds) > 0:
        asyncio.run(
            send_discord_embed(
                contents=all_embeds,
                username="Monitoramento HCI",
                monitor_slug="hci_status",
            )
        )

    ####################################
    ## Alertas
    ####################################
    ROLE_ID = "1224334248345862164"
    ROLE_MENTION = f"<@&{ROLE_ID}>"
    if len(warnings) > 0:
        log(f"Sending warnings:{warnings}", level="warning")
        message = f"**Alertas** ({ROLE_MENTION}):\n"
        message += "\n".join(warnings)
        asyncio.run(
            send_discord_webhook(
                message=message,
                username="Monitoramento HCI",
                monitor_slug="hci_status",
            )
        )
