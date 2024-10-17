# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import load_file_from_bigquery


@task
def get_target_date(data, target_date):
    if not target_date:
        target_date = data["data_atualizacao"].max()
        log(f"Target date not provided. Using latest date available: {target_date}")
    else:
        log(f"Target date provided: {target_date}")
    return target_date


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
def create_markdown_report_from_source(data, source, target_date):
    data = data[data["data_atualizacao"] == target_date]

    date_readable = pd.to_datetime(target_date).strftime("%d/%m/%Y")

    if data.empty:
        log("No data to report")
        return

    data_from_source = data[data["fonte"] == source]

    message_lines = [
        f"Relatório de Ingestão de Dados - {source.upper()} no dia {date_readable})"
    ]  # noqa
    for type in data_from_source["tipo"].unique():
        message_lines.append(f"### {type.capitalize()}")

        filtered_data = data_from_source[data_from_source["tipo"] == type]
        filtered_data = filtered_data.to_dict(orient="records")[0]

        units_without_data = list(
            sorted(filtered_data["unidades_sem_dado"], key=lambda x: x["unidade_ap"])
        )  # noqa
        message_lines.append(f"- 🚨 **Unidades sem dados:** {len(units_without_data)}")

        for i, unit in enumerate(units_without_data):
            ap = unit["unidade_ap"]
            name = unit["unidade_nome"]
            cnes = unit["unidade_cnes"]
            message_lines.append(f"> `[AP{ap}] {name} ({cnes})`")

        units_with_data = filtered_data["unidades_com_dado"]
        message_lines.append(f"- 📊 **Unidades com dados:** {len(units_with_data)}")

        for i, unit in enumerate(units_with_data):
            ap = unit["unidade_ap"]
            name = unit["unidade_nome"]
            cnes = unit["unidade_cnes"]
            message_lines.append(f"> `[AP{ap}] {name} ({cnes})`")

    message_lines.append("")

    log("\n".join(message_lines))

    txt_path = "./report.md"
    with open(txt_path, "w") as f:
        f.write("\n".join(message_lines))

    return txt_path


@task
def send_report(data, target_date):
    data = data[data["data_atualizacao"] == target_date]
    date_readable = pd.to_datetime(target_date).strftime("%d/%m/%Y")

    if data.empty:
        log("No data to report")
        return

    for source in data["fonte"].unique():
        title = f"Relatório de Ingestão de Dados - {source.upper()} no dia {date_readable})"

        data_from_source = data[data["fonte"] == source]

        txt_report_path = create_markdown_report_from_source.run(
            data=data, source=source, target_date=target_date
        )

        message_lines = ["Unidades sem Dado:"]
        for type in data_from_source["tipo"].unique():

            filtered_data = data_from_source[data_from_source["tipo"] == type]
            filtered_data = filtered_data.to_dict(orient="records")[0]

            units_without_data = filtered_data["unidades_sem_dado"]
            units_with_data = filtered_data["unidades_com_dado"]

            proportion = len(units_without_data) / (len(units_without_data) + len(units_with_data))
            percent = round(proportion * 100, 1)
            emoji = "🔴" if percent > 5 else "🟢"

            message_lines.append(
                f"- {emoji} {type.capitalize()}: {len(units_without_data)} ({percent}%)"
            )

        log(f"Sending message with {len(message_lines)} lines")
        log("\n".join(message_lines))

        send_message(
            title=title,
            message="\n".join(message_lines),
            file_path=txt_report_path,
            monitor_slug="data-ingestion",
        )
