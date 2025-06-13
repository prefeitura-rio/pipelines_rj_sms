# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Optional
import pandas as pd
from google.cloud import bigquery
import pytz

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

    # Quantidade de intervalos de 30 minutos em 7 dias
    denom = 7 * 24 * 2
    QUERY_7D = get_query(interval="7 DAY", denom=denom)
    rows_7d = [row.values() for row in client.query(QUERY_7D).result()]
    log(f"Query for tipo_evento, resultado (7 d) returned {len(rows_7d)} row(s)")
    # Lista de tuplas (evento, quantidade)
    return (rows_30m, rows_7d)


@task
def send_report(data):
    data_30m = data[0]
    data_7d = data[1]

    # Se estamos fora do horário de trabalho, é esperado que tenhamos
    # poucos acessos; não vamos reportar erros de pouco acesso
    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    REPORT_LOW_ACCESS = (now.hour >= 7 and now.hour <= 19 and now.weekday() < 5)

    UID = ""
    USER_MENTION = f"<@{UID}>"

    def build_event_dict(data):
        events = dict()
        for event in data:
            endpoint = event[0]
            status = event[1]
            average = event[2]
            if endpoint not in events:
                events[endpoint] = {}
            events[endpoint][status] = average
        return events

    events_30m = build_event_dict(data_30m)
    events_7d = build_event_dict(data_7d)
    log(events_30m)
    log(events_7d)

    # Para cada evento recente (events_30m):
    # - Se for HTTP 500, avisa
    # - Se for HTTP 200, confere se está próximo à média semanal


    # Além disso:
    # - Verifica se só temos logins/buscas/etc, mas não consultas
    types = [event_type.lower() for event_type, event_instances in events_30m.items() if "200" in event_instances]
    actual_usage_count = len([t for t in types if t.startswith("consulta")])
    if actual_usage_count <= 0:
        log("No 'consulta' event type!", level="warning")


    # send_discord_webhook(
    #     title=...,
    #     message=...,
    #     username=...,
    #     monitor_slug="warnings",
    # )
