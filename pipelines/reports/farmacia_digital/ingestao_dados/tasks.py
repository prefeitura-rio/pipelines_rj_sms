# -*- coding: utf-8 -*-
from pipelines.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import load_file_from_bigquery


@task
def get_data(environment, target_date):

    data = load_file_from_bigquery.run(
        project_name=constants.GOOGLE_CLOUD_PROJECT.value[environment],
        dataset_name="gerenciamento__monitoramento",
        table_name="estatisticas_consolidadas",
        environment=environment,
    )

    if not target_date:
        target_date = data["data_atualizacao"].max()

    return data[data["data_atualizacao"] == target_date]


@task
def send_report(data, target_date):
    UNIT_LINE_LIMIT = 5

    date_readable = target_date.strftime("%d/%m/%Y")

    if data.empty:
        log("No data to report")
        return

    for source in data["fonte"].unique():
        title = f"Relatório de Ingestão de Dados - {source.upper()} no dia {date_readable})"

        data_from_source = data[data["fonte"] == source]

        message_lines = []

        def add_line(line, level="info"):
            assert level in ["info", "debug", "only_debug"]
            message_lines.append((level, line))

        for type in data["tipo"].unique():
            add_line(f"### {type.capitalize()}")

            filtered_data = data_from_source[data_from_source["tipo"] == type]
            filtered_data = filtered_data.to_dict(orient="records")[0]

            units_with_data = filtered_data["unidades_com_dado"]
            add_line(f"- 📊 **Unidades com dados:** {len(units_with_data)}")

            for i, unit in enumerate(units_with_data):
                ap = unit["unidade_ap"]
                name = unit["unidade_nome"]
                cnes = unit["unidade_cnes"]
                add_line(f"> `[AP{ap}] {name} ({cnes})`", "debug")

            units_without_data = list(
                sorted(filtered_data["unidades_sem_dado"], key=lambda x: x["unidade_ap"])
            )  # noqa
            add_line(f"- 🚨 **Unidades sem dados:** {len(units_without_data)}")

            for i, unit in enumerate(units_without_data):
                ap = unit["unidade_ap"]
                name = unit["unidade_nome"]
                cnes = unit["unidade_cnes"]

                if i > UNIT_LINE_LIMIT:
                    add_line(f"> `[AP{ap}] {name} ({cnes})`", "debug")
                else:
                    add_line(f"> `[AP{ap}] {name} ({cnes})`")

            if len(units_without_data) > UNIT_LINE_LIMIT:
                add_line(
                    f"> ... e mais {len(units_without_data) - UNIT_LINE_LIMIT} unidades sem dados.",
                    "only_debug",
                )  # noqa

        txt_message = [line for level, line in message_lines if level != "only_debug"]
        with open("report.md", "w") as f:
            f.write("\n".join(txt_message))

        discord_message = [line for level, line in message_lines if level != "debug"]
        send_message(
            title=title,
            message="\n".join(discord_message),
            file_path="report.md",
            monitor_slug="data-ingestion",
        )
