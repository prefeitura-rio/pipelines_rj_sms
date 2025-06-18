# -*- coding: utf-8 -*-
import asyncio
from datetime import datetime
from typing import List, Optional

import pandas as pd
import pytz
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_discord_webhook
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
    # 7 (dias) x 8 (horas) x 2 (meia-horas)
    denom = 7 * 8 * 2
    QUERY_7D = get_query(interval="7 DAY", denom=denom)
    rows_7d = [row.values() for row in client.query(QUERY_7D).result()]
    log(f"Query for tipo_evento, resultado (7 d) returned {len(rows_7d)} row(s)")
    # Lista de tuplas (evento, quantidade)
    return (rows_30m, rows_7d)


@task
def send_report(data):
    data_30m = data[0]
    data_7d = data[1]

    log(data_30m)
    log(data_7d)

    UID = ""
    USER_MENTION = f"<@{UID}>"

    to_warn = []
    to_notify = []
    # Para cada evento recente:
    for event in data_30m:
        evt_type: str = event[0]
        status: str = event[1]
        amount: float = event[2]
        if amount <= 0:
            continue
        if 'desenvolvimento' in evt_type.lower():
            continue

        s = "" if amount < 2 else "s"
        # Se for HTTP 500, avisa
        if status == '500':
            to_warn.append(f"🚨 '{evt_type}' (500): {amount:.0f} ocorrência{s}")
            continue

        # Se for HTTP 200, confere se está próximo à média semanal
        average = next((
            average
            for (endpoint_7d, status_7d, average) in data_7d
            if endpoint_7d  == evt_type and status_7d == status
        ), 0)
        # TODO: 10?
        to_notify.append(f"'{evt_type}' ({status}): {amount:.0f} ocorrência{s} (média: {average:.2f})")

    # Além disso:
    # - Verifica se só temos logins/buscas/etc, mas não consultas
    types = [
        evt_type.lower()
        for (evt_type, status, _) in data_30m
        if status == "200"
    ]
    actual_usage_count = len([t for t in types if t.startswith("consulta")])
    if actual_usage_count <= 0:
        to_warn.append(f"🚨 Nenhuma consulta no intervalo avaliado!")


    message = ""
    to_notify.sort()
    if len(to_notify) > 0:
        outstr = "\n* ".join(to_notify)
        message += "* " + outstr if len(outstr) else ""
        log(f"Sending notification:{outstr}")

    to_warn.sort()
    if len(to_warn) > 0:
        message += "\n\n"
        outstr = "\n* ".join(to_warn)
        message += "* " + outstr if len(outstr) else ""
        log(f"Sending warning:{outstr}", level="warning")

    # TODO: Teste de requisição da API diretamente

    asyncio.run(
        send_discord_webhook(
            text_content=message,
            username="Monitoramento HCI",
            monitor_slug="warning",
        )
    )
