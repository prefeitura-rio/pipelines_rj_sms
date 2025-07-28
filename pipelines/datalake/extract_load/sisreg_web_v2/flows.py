# -*- coding: utf-8 -*-
"""
Fluxo para extração e carga de dados do SISREG.
"""

# bibliotecas do prefect
from prefect import Parameter
from prefect.run_configs import VertexRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.sisreg_web_v2.schedules import (
    sisreg_daily_update_schedule,
)
from pipelines.datalake.extract_load.sisreg_web_v2.tasks import (
    login_sisreg,
    nome_arquivo_adicionar_data,
    raspar_pagina,
    sisreg_encerrar,
    transform_data,
)

# módulos internos
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import create_folders, create_partitions, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - SISREG v.2",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
        constants.MATHEUS_ID.value,
    ],
) as sms_sisreg:

    # ambiente
    ENVIRONMENT = Parameter("environment", default="dev")

    # tarefas sisreg
    RELATIVE_PATH = Parameter("relative_path", default="downloaded_data/")
    SISREG_METHOD = Parameter("sisreg_method", default="baixar_oferta_programada")

    # big query
    DATASET_ID = Parameter("dataset_id", default="brutos_sisreg_v2")
    TABLE_ID = Parameter("table_id", default="oferta_programada")

    # --------------------------------------

    # configurando o ambiente
    local_folders = create_folders()

    # tarefa 1: login
    sisreg, caminho_download = login_sisreg(environment=ENVIRONMENT, caminho_relativo=RELATIVE_PATH)

    # tarefa 2: obter dados
    arquivo_baixado = raspar_pagina(
        sisreg=sisreg, caminho_download=caminho_download, metodo=SISREG_METHOD
    )

    # tarefa 3: adicionar data atual no nome do arquivo
    arquivo_baixado_data = nome_arquivo_adicionar_data(arquivo_original=arquivo_baixado)

    # tarefa 4: encerrar sessão do sisreg
    sisreg_encerrar(sisreg=sisreg, upstream_tasks=[arquivo_baixado])

    # tarefa 5: transformar os dados e converter para parquet
    arquivo_parquet = transform_data(file_path=arquivo_baixado_data)

    # tarefa 6: criar partições
    particoes_tarefa = create_partitions(
        data_path=arquivo_parquet,
        partition_directory=local_folders["partition_directory"],
        upstream_tasks=[arquivo_parquet],
    )

    # tarefa 7: subir o arquivo para o datalake
    upload_to_datalake_task = upload_to_datalake(
        input_path=local_folders["partition_directory"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode="append",
        source_format="parquet",
        if_exists="replace",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[particoes_tarefa],
    )


# --------------------------------------

# configurações padrão do fluxo
sms_sisreg.schedule = sisreg_daily_update_schedule
sms_sisreg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_sisreg.run_config = VertexRun(
    image=constants.DOCKER_VERTEX_IMAGE.value,
    labels=[constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": constants.INFISICAL_TOKEN.value,
    },
)
