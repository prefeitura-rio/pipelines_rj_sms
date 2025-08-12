# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import pytz
from dateutil import parser
from google.cloud import storage

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.time import from_relative_date


@task
def get_data_from_gcs_bucket(configuration: dict, environment: str):
    client = storage.Client()
    bucket = client.get_bucket(configuration["bucket_name"])
    blobs = bucket.list_blobs()

    relative_date = from_relative_date.run(relative_date=configuration["source_freshness"])
    relative_date = parser.parse(relative_date.isoformat())
    relative_date = pytz.timezone("America/Sao_Paulo").localize(relative_date)
    files = [x for x in blobs if x.time_created > relative_date]

    log(f"[{environment}] Quantidade de arquivos no período selecionado: {len(files)}")

    return len(files)


@task
def send_report(configurations: list[dict], results: list[int], environment: str):
    base_date_readable = pd.to_datetime(datetime.now()).strftime("%d/%m/%Y")
    title = f"Relatório de arquivos do GCS - {base_date_readable}"

    message_lines = []
    for configuration, result in zip(configurations, results):
        if result == 0:
            emoji = "🔴"
        else:
            emoji = "🟢"

        source = configuration["title"]
        subtitle = f"{source} (`gs://{configuration['bucket_name']}` - `{configuration['source_freshness']}`)"  # noqa
        message_lines.append(f"- {emoji} {subtitle}: {result} arquivos.")

    log(f"[{environment}] Sending message with {len(message_lines)} line(s).")

    send_message(
        title=title,
        message="\n".join(message_lines),
        monitor_slug="data-ingestion",
    )
