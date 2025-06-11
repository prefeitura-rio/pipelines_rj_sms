# -*- coding: utf-8 -*-
import pandas as pd
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_bigquery_project_from_environment


@task
def get_data(field: str, dataset_name: str, table_name: str, interval: str, environment: str):
    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment="prod")

    QUERY = f"""
select {field}, count(*)
from `{project_name}.{dataset_name}.{table_name}`
where created_at >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL {interval})
group by {field}
        """
    query_job = client.query(QUERY)
    rows = [row.values() for row in query_job.result()]
    log(f"Query for '{field}' (INTERVAL {interval}) returned {len(rows)} row(s)")
    # Lista de tuplas (evento, quantidade)
    return rows


@task
def send_report(data):
    endpoints = data[0]
    http_data = data[1]
    log(endpoints)
    # `endpoints`` é algo como:
    # ('evento1', 22),
    # ('evento3', 9),
    # ('evento4', 3),

    log(http_data)
    # `http_data` é algo como:
    # ('200', 91), ('403', 4), ('400', 2), ('401', 1), ('500', 1)

    # TODO: queremos detectar se algo inesperado aconteceu
    # exs.: somente um tipo de evento acontecendo
    #       quantidade anômala de status HTTP 500
    # Importante: verificar data e hora atual;
    # - esperamos menos uso do HCI fora do horário de trabalho
    # - ''        ''    ''  '' '' em sábados e domingos

    # log(f"Sending message with {len(message_lines)} lines")
    # log("\n".join(message_lines))

    # send_message(
    #     title=title,
    #     message="\n".join(message_lines),
    #     monitor_slug="data-ingestion",
    # )
