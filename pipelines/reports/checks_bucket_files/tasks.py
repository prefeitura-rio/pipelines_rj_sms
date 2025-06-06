# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import pytz
from dateutil import parser
from google.cloud import storage

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def get_data_from_gcs_bucket(bucket_name: str, environment: str, relative_date: pd.Timestamp):

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    relative_date = parser.parse(relative_date.isoformat())
    relative_date = pytz.timezone("America/Sao_Paulo").localize(relative_date)
    files = [x for x in blobs if x.time_created > relative_date]

    log(f"Quantidade de arquivos no período selecionado:{len(files)}")

    return len(files)


@task
def send_report(bucket_name: str, source_freshness: str, len_files: int):

    base_date_readable = pd.to_datetime(datetime.now()).strftime("%d/%m/%Y")

    if "vitacare" in bucket_name:
        source = "VITACARE"
    elif "cgcca" in bucket_name:
        source = "CGCCA"
    elif "aps" in bucket_name:
        source = "APS"

    title = f"Relatório de arquivos do GCS -  {source} - {base_date_readable}"

    message_lines = []

    if len_files == 0:
        emoji = "🔴"
    else:
        emoji = "🟢"

    message_lines.append(f"{emoji} {bucket_name} ({source_freshness}): {len_files} arquivos.")

    log(f"Sending message with {len(message_lines)} line(s).")

    send_message(
        title=title,
        message="\n".join(message_lines),
        monitor_slug="data-ingestion",
    )
