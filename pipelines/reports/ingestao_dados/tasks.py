# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import load_file_from_bigquery


@task
def get_base_date(base_date):
    log("Getting target date")
    if base_date == "today" or not base_date:
        base_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        log(f"Using today's date: {base_date}")
    elif base_date == "yesterday":
        base_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"Using yesterday's date: {base_date}")
    else:
        base_date = pd.Timestamp(base_date).strftime("%Y-%m-%d")
        log(f"Using specified date: {base_date}")

    return base_date


@task
def get_data(environment):

    data = load_file_from_bigquery.run(
        project_name=constants.GOOGLE_CLOUD_PROJECT.value[environment],
        dataset_name="gerenciamento__monitoramento",
        table_name="estatisticas_consolidadas",
        environment=environment,
    )

    log(f"Data loaded from BigQuery: {data.shape[0]} rows")
    return data


@task
def send_report(data, base_date):
    for source in data["fonte"].unique():
        data_from_source = data[data["fonte"] == source]

        base_date_readable = pd.to_datetime(base_date).strftime("%d/%m/%Y")
        title = f"Relatório de Ingestão de Dados - {source.upper()} - {base_date_readable}"

        message_lines = ["Unidades sem Dado:"]
        for type in data_from_source["tipo"].unique():

            if type in ["posicao"]:
                reference_code = "D-0"
                reference_date = pd.to_datetime(base_date)
            elif type in ["vacina"]:
                reference_code = "D-3"
                reference_date = pd.to_datetime(base_date) - pd.Timedelta(days=3)
            else:
                reference_code = "D-1"
                reference_date = pd.to_datetime(base_date) - pd.Timedelta(days=1)

            filtered_data = data_from_source[
                (data_from_source["data_ingestao"] == reference_date)
                & (data_from_source["tipo"] == type)
            ]

            if filtered_data.empty:
                log("No data to report")
                return

            filtered_data = filtered_data.to_dict(orient="records")[0]

            units_without_data = filtered_data["unidades_sem_dado"]
            units_with_data = filtered_data["unidades_com_dado"]

            proportion = len(units_without_data) / (len(units_without_data) + len(units_with_data))
            percent = round(proportion * 100, 1)

            emoji = "?"
            if percent > 5:
                emoji = "🔴"
            elif percent > 0:
                emoji = "🟡"
            else:
                emoji = "🟢"

            message_lines.append(
                f"- {emoji} {type.capitalize()} ({reference_code}): {len(units_without_data)} ({percent}%)" # noqa
            )

        log(f"Sending message with {len(message_lines)} lines")
        log("\n".join(message_lines))

        send_message(
            title=title,
            message="\n".join(message_lines),
            monitor_slug="data-ingestion",
        )
