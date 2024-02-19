# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
CNES dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.dump_ftp_cnes.constants import constants as cnes_constants
from pipelines.dump_ftp_cnes.schedules import every_sunday_at_six_am
from pipelines.dump_ftp_cnes.tasks import (
    add_multiple_date_column,
    check_file_to_download,
    conform_csv_to_gcp,
    convert_csv_to_parquet,
    create_partitions_and_upload_multiple_tables_to_datalake,
    rename_current_flow_run_cnes,
)
from pipelines.utils.tasks import (
    create_folders,
    download_ftp,
    inject_gcp_credentials,
    unzip_file,
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
    file_to_download_task.set_upstream(inject_gcp_credentials_task)

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

    download_task = download_ftp(
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

    unzip_task = unzip_file(file_path=download_task, output_path=create_folders_task["raw"])
    unzip_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(create_folders_task["raw"])
    conform_task.set_upstream(unzip_task)  # pylint: disable=E1101

    add_multiple_date_column_task = add_multiple_date_column(
        directory=create_folders_task["raw"],
        snapshot_date=file_to_download_task["snapshot"],
        sep=";",
    )
    add_multiple_date_column_task.set_upstream(conform_task)

    convert_csv_to_parquet_task = convert_csv_to_parquet(
        directory=create_folders_task["raw"], sep=";"
    )
    convert_csv_to_parquet_task.set_upstream(add_multiple_date_column_task)

    upload_to_datalake_task = create_partitions_and_upload_multiple_tables_to_datalake(
        path_files=convert_csv_to_parquet_task,
        partition_folder=create_folders_task["partition_directory"],
        partition_date=file_to_download_task["snapshot"],
        dataset_id=DATASET_ID,
        dump_mode="append",
    )
    upload_to_datalake_task.set_upstream(add_multiple_date_column_task)


sms_dump_cnes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_cnes.executor = LocalDaskExecutor(num_workers=10)
sms_dump_cnes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="8Gi",
)

sms_dump_cnes.schedule = every_sunday_at_six_am
