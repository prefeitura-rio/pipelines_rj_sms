# -*- coding: utf-8 -*-

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GCS

from pipelines.constants import constants as pipeline_constants
from pipelines.utils.flow import Flow
from pipelines.utils.tasks import get_secret_key
from pipelines.utils.state_handlers import handle_flow_state_change

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.constants import (
    INFISICAL_PATH,
    INFISICAL_USERNAME,
    INFISICAL_PASSWORD,
    DATASET_ID_BRUTO,
    TABLE_ID_BP,
)

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.tasks import (
    gerar_roteiro_extracao,
    obter_sessao_autenticada,
    extrair_item_sisreg,
    consolidar_e_salvar,
)

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.schedules import schedule

with Flow(
    name="SUBGERAL/CR/NTI - Extract & Load - SISREG Solicitações",
    state_handlers=[handle_flow_state_change],
) as sisreg_bp_flow:

    ENVIRONMENT = Parameter("environment", default="dev")
    data_especifica = Parameter("data_especifica", default=None)
    data_inicial = Parameter("data_inicial", default=None)
    data_final = Parameter("data_final", default=None)
    status_especifico = Parameter("status_especifico", default=None)

    timeout_login = Parameter("timeout_login", default=30)
    timeout_consulta = Parameter("timeout_consulta", default=180)
    tempo_espera_sisreg = Parameter("tempo_espera_sisreg", default=8.5)

    usuario_infisical = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_USERNAME,
        environment=ENVIRONMENT,
    )

    senha_infisical = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_PASSWORD,
        environment=ENVIRONMENT,
    )

    sessao_autenticada = obter_sessao_autenticada(
        usuario=usuario_infisical,
        senha=senha_infisical,
        timeout=timeout_login,
    )

    lista_fichas_extracao = gerar_roteiro_extracao(
        data_especifica=data_especifica,
        data_inicial=data_inicial,
        data_final=data_final,
        status_especifico=status_especifico,
    )

    resultados_em_massa = extrair_item_sisreg.map(
    sessao=unmapped(sessao_autenticada),
    usuario=unmapped(usuario_infisical),
    senha=unmapped(senha_infisical),
    ficha=lista_fichas_extracao,
    timeout_login=unmapped(timeout_login),
    timeout_consulta=unmapped(timeout_consulta),
    tempo_espera=unmapped(tempo_espera_sisreg),
    )

    consolidar_e_salvar(
        lista_de_dfs=resultados_em_massa,
        dataset_id=DATASET_ID_BRUTO,
        table_id=TABLE_ID_BP,
    )

# Configuração do Vertex
sisreg_bp_flow.executor = LocalDaskExecutor(num_workers=1)
sisreg_bp_flow.storage = GCS(pipeline_constants.GCS_FLOWS_BUCKET.value)

sisreg_bp_flow.run_config = VertexRun(
    image=pipeline_constants.DOCKER_VERTEX_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": pipeline_constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": pipeline_constants.INFISICAL_TOKEN.value,
    },
)

sisreg_bp_flow.schedule = schedule
