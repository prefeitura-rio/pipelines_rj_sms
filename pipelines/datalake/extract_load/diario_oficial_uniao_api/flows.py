# -*- coding: utf-8 -*-
# pylint: disable=C0103

from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_uniao_api.constants import (
    constants as flow_constants,
)
from pipelines.datalake.extract_load.diario_oficial_uniao_api.tasks import (
    create_dirs,
    delete_dirs,
    download_files,
    get_xml_files,
    login,
    parse_date,
    report_extraction_status,
    unpack_zip,
    upload_to_datalake,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Extração e Carga de Dados - Diário Oficial da União (API)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as dou_extraction:
    """
    Fluxo de extração e carga de atos oficiais do Diário Oficial da União (DOU).
    """

    #####################################
    # Parameters
    #####################################

    ENVIRONMENT = Parameter("environment", default="dev")
    DATE = Parameter("date", default="")
    DOU_SECTION = Parameter("dou_section", default="DO1 DO2 DO3")
    DATASET_ID = Parameter("dataset_id", default="brutos_diario_oficial")

    #####################################
    # Flow
    #####################################

    create_dirs()

    parsed_date = parse_date(date=DATE)

    # Realiza o login
    session = login(enviroment=ENVIRONMENT, upstream_tasks=[parsed_date, create_dirs])

    # Faz o download dos arquivos .zip com os atos oficiais de cada seção
    zip_files = download_files(
        session=session, sections=DOU_SECTION, date=parsed_date, upstream_tasks=[session]
    )

    # Descompacta os arquivos .zip
    flag = unpack_zip(
        zip_files=zip_files,
        output_path=flow_constants.OUTPUT_DIR.value,
        upstream_tasks=[zip_files, create_dirs],
    )

    # Pega as informações dos xml de cada ato oficial
    parquet_file = get_xml_files(
        xml_dir=flow_constants.OUTPUT_DIR.value, upstream_tasks=[flag, create_dirs]
    )

    # Faz o upload para o bigquery
    upload_status = upload_to_datalake(
        parquet_path=parquet_file, dataset=DATASET_ID, upstream_tasks=[parquet_file]
    )

    # Reportando status da extração
    report_extraction_status(
        status=upload_status,
        date=DATE,
        environment=ENVIRONMENT,
        upstream_tasks=[upload_status, upload_status],
    )

    delete_dirs(upstream_tasks=[parquet_file, upload_status])


# Flow configs
dou_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dou_extraction.executor = LocalDaskExecutor(num_workers=1)
dou_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
    memory_request="2Gi",
)
