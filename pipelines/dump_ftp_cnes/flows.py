# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
CNES dumping flows
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.executors import LocalDaskExecutor
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants
from pipelines.dump_ftp_cnes.constants import (
    constants as cnes_constants,
)
from pipelines.utils.tasks import (
    inject_gcp_credentials,
    create_folders,
    unzip_file,
)
from pipelines.dump_ftp_cnes.tasks import (
    rename_current_flow_run_cnes, 
    check_file_to_download,
    download_ftp_cnes,
    conform_csv_to_gcp,
    add_multiple_date_column,

)
from pipelines.dump_api_vitai.schedules import (
    vitai_daily_update_schedule,
)


with Flow(
    name="Dump CNES - Ingerir dados do CNES",
) as sms_dump_cnes:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # CNES
    FTP_SERVER = cnes_constants.FTP_SERVER.value
    FTP_FILE_PATH = cnes_constants.FTP_FILE_PATH.value
    BASE_FILE = cnes_constants.BASE_FILE.value
    DOWNLOAD_NEWEST_FILE = Parameter("download_newest", default=True)

    # Aditional parameters for CNES if download_newest is False
    FILE_TO_DOWNLOAD = Parameter("file_to_download", default=None, required=False)
    PARTITION_DATE = Parameter(
        "partition_date", default=None, required=False
    )  # format YYYY-MM-DD or YY-MM. Partitions level must follow

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=cnes_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    ####################################
    # Tasks section #1 - Get file to download
    #####################################

    file_to_download_task = check_file_to_download(
        download_newest=DOWNLOAD_NEWEST_FILE,
        file_to_download=FILE_TO_DOWNLOAD,
        partition_date=PARTITION_DATE,
        host=FTP_SERVER,
        user="",
        password="",
        directory=FTP_FILE_PATH,
        file_name=BASE_FILE,
    )

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_cnes(
            prefix="Dump CNES: ", file=file_to_download_task["file"]
        )
        rename_flow_task.set_upstream(inject_gcp_credentials_task)

    ####################################
    # Tasks section #2 - Get data
    #####################################
        
    create_folders_task = create_folders()
    create_folders_task.set_upstream(file_to_download_task)  # pylint: disable=E1101

    download_task = download_ftp_cnes(
        host=FTP_SERVER,
        user="",
        password="",
        directory=FTP_FILE_PATH,
        file_name=file_to_download_task["file"],
        output_path=create_folders_task["raw"],
    )
    download_task.set_upstream(create_folders_task)

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    unzip_task = unzip_file(
        file_path=download_task, output_path=create_folders_task["raw"]
    )
    unzip_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(create_folders_task["raw"])
    conform_task.set_upstream(unzip_task)  # pylint: disable=E1101

    add_multiple_date_column_task = add_multiple_date_column(
        directory=create_folders_task["raw"],
        snapshot_date=file_to_download_task["snapshot"],
        sep=";",
    )
    add_multiple_date_column_task.set_upstream(conform_task)

    # TODO: salvar como parquet

    # TODO: mudar como a partição mês é persistida para ficar conforme com o padrão do Escritório

    # TODO: mudar a task para criar a tabela no BigQuery

    # TODO: salver no BQ usando o pacote do Prefeitura Rio


sms_dump_cnes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_cnes.executor = LocalDaskExecutor(num_workers=10)
sms_dump_cnes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi"
)

sms_dump_cnes.schedule = vitai_daily_update_schedule
