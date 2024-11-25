# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for Farmácia Digital - Livro de Medicamentos Controlados
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.reports.farmacia_digital.livro_controlados.livro_controlados.schedules import (
    weekly_schedule,
)
from pipelines.reports.farmacia_digital.livro_controlados.tasks import (
    generate_report,
    get_google_drive_folder_id,
    get_reference_date,
    upload_report_to_gdrive,
)
from pipelines.utils.tasks import create_folders

with Flow(
    name="Report: Farmácia Digital - Livro de Medicamentos Controlados",
) as report_farmacia_digital_livro_controlados:

    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # report
    DATA_COMPETENCIA = Parameter(name="data_competencia", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    competencia = get_reference_date(competencia=DATA_COMPETENCIA)

    google_drive_folder_id = get_google_drive_folder_id(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            competencia=competencia,
        )

    ####################################
    # Tasks section #1 - Generate report
    ####################################

    reports = generate_report(output_directory=local_folders["raw"], competencia=competencia)
    reports.set_upstream(local_folders)

    # Upload to Google Drive
    upload_task = upload_report_to_gdrive(
        folder_path=local_folders["raw"],
        folder_id=google_drive_folder_id,
    )
    upload_task.set_upstream(reports)


# report_farmacia_digital_livro_controlados.schedule = weekly_schedule
report_farmacia_digital_livro_controlados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_farmacia_digital_livro_controlados.executor = LocalDaskExecutor(num_workers=1)
report_farmacia_digital_livro_controlados.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
