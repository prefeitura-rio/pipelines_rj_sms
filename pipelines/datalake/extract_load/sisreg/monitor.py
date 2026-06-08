# -*- coding: utf-8 -*-
"""
Monitor de frescor das execucoes SISREG.

Verifica a tabela de log de execucoes e dispara alerta no Discord quando
algum conjunto nao teve execucao bem-sucedida dentro do SLA definido.

A falha de agendamento (agente parado, flow desregistrado) nao produz
erros no Prefect, portanto o state_handler nao alerta - e necessario um
monitor externo que verifique a AUSENCIA de execucoes bem-sucedidas.

Funcoes exportadas:
  - verificar_frescor_conjuntos: task Prefect para uso no DAG do fluxo.
  - alertar_frescor_discord: envia mensagem formatada ao Discord.
"""

from datetime import datetime, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from prefect import task
from prefect.triggers import all_finished
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.utils.monitor import send_message

_FUSO_SP = ZoneInfo("America/Sao_Paulo")


def _calcular_sla_violacoes(
    df_log: pd.DataFrame,
    agora: datetime,
) -> List[dict]:
    """Identifica conjuntos cujo SLA de frescor foi violado.

    Args:
        df_log: DataFrame da tabela de log com colunas 'conjunto', 'as_of', 'status'.
        agora: Timestamp atual (fuso SP).

    Returns:
        Lista de dicts com {'conjunto', 'ultimo_sucesso', 'sla_dias', 'horas_atraso'}.
    """
    violacoes: List[dict] = []

    for conjunto, sla_dias in C.SLA_FRESCOR_DIAS.items():
        df_conj = df_log[(df_log["conjunto"] == conjunto) & (df_log["status"] == "OK")]

        if df_conj.empty:
            violacoes.append(
                {
                    "conjunto": conjunto,
                    "ultimo_sucesso": None,
                    "sla_dias": sla_dias,
                    "horas_atraso": sla_dias * 24,
                }
            )
            continue

        # Converte as_of para datetime se vier como string
        ultimo_ok = pd.to_datetime(df_conj["as_of"]).max()
        if hasattr(ultimo_ok, "to_pydatetime"):
            ultimo_ok = ultimo_ok.to_pydatetime()

        # Garante fuso horario para comparacao
        if ultimo_ok.tzinfo is None:
            ultimo_ok = ultimo_ok.replace(tzinfo=_FUSO_SP)

        horas_desde_ultimo = (agora - ultimo_ok).total_seconds() / 3600
        limite_horas = sla_dias * 24

        if horas_desde_ultimo > limite_horas:
            violacoes.append(
                {
                    "conjunto": conjunto,
                    "ultimo_sucesso": ultimo_ok.isoformat(),
                    "sla_dias": sla_dias,
                    "horas_atraso": round(horas_desde_ultimo - limite_horas, 1),
                }
            )

    return violacoes


def alertar_frescor_discord(violacoes: List[dict]) -> None:
    """Envia alerta de violacao de SLA ao canal data-ingestion do Discord.

    Chamado apenas quando ha violacoes. O canal 'data-ingestion' e o adequado
    para alertas de saude de dados (nao 'error', que e reservado para falhas
    de fluxo via state_handler).
    """
    linhas = [f"**Monitor de Frescor SISREG** - {len(violacoes)} violacao(es) de SLA:\n"]
    for v in violacoes:
        ultimo = v["ultimo_sucesso"] or "nunca"
        linhas.append(
            f"- `{v['conjunto']}`: SLA={v['sla_dias']}d, "
            f"ultimo_sucesso={ultimo}, atraso={v['horas_atraso']}h"
        )
    mensagem = "\n".join(linhas)

    try:
        send_message(
            title="SISREG - Violacao de SLA de Frescor",
            message=mensagem,
            monitor_slug="data-ingestion",
        )
        log("[monitor] Alerta de frescor enviado ao Discord")
    except Exception as exc:  # noqa: BLE001
        log(f"[monitor] Falha ao enviar alerta (nao critico): {exc}", level="warning")


@task(trigger=all_finished)
def verificar_frescor_conjuntos(
    dataset_id: str,
    environment: str,
) -> Optional[List[dict]]:
    """Verifica o frescor de todos os conjuntos e alerta se houver violacoes de SLA.

    Le a tabela de log de execucoes, calcula quais conjuntos estao em atraso
    e envia alerta ao Discord. Projetado para rodar ao final de cada execucao
    do fluxo (trigger=all_finished).

    Em ambiente dev/staging, apenas loga sem enviar alerta para evitar ruido.

    Args:
        dataset_id: Dataset onde a tabela de log esta armazenada.
        environment: "prod" ou "staging"/"dev".

    Returns:
        Lista de violacoes encontradas, ou None em erro de leitura.
    """
    from google.cloud import (
        bigquery,  # importacao tardia - nao disponivel em testes offline
    )

    agora = datetime.now(_FUSO_SP)
    janela_consulta = agora - timedelta(days=max(C.SLA_FRESCOR_DIAS.values()) + 1)

    sql = f"""
    SELECT conjunto, as_of, status
    FROM `{dataset_id}.{C.TABELA_LOG_EXECUCAO}`
    WHERE {C.COLUNA_PARTICAO} >= '{janela_consulta.strftime("%Y-%m-%d")}'
    ORDER BY as_of DESC
    """

    try:
        cliente = bigquery.Client()
        df_log = cliente.query_and_wait(sql).to_dataframe()
    except Exception as exc:  # noqa: BLE001
        log(f"[monitor] Nao foi possivel ler a tabela de log: {exc}", level="warning")
        return None

    violacoes = _calcular_sla_violacoes(df_log, agora)

    if not violacoes:
        log("[monitor] Todos os conjuntos estao dentro do SLA")
        return []

    log(
        f"[monitor] {len(violacoes)} violacao(es) de SLA detectadas: "
        f"{[v['conjunto'] for v in violacoes]}"
    )

    if environment == "prod":
        alertar_frescor_discord(violacoes)
    else:
        log(f"[monitor] Ambiente={environment}: alerta suprimido (nao-prod)")

    return violacoes
