# -*- coding: utf-8 -*-

from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GCS

from pipelines.constants import constants as pipeline_constants
from pipelines.utils.flow import Flow
from pipelines.utils.tasks import get_secret_key
from pipelines.utils.state_handlers import handle_flow_state_change

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.constants import (
    INFISICAL_PATH,
    INFISICAL_VITACARE_USERNAME,
    INFISICAL_VITACARE_PASSWORD,
    DATASET_ID_BRUTO,
    TABLE_ID_BP
)

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.tasks import (
    gerar_roteiro_extracao,
    extrair_fase_principal,    
    extrair_fase_reextracao,   
    salvar_resultados
)

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.schedules import schedule


with Flow(
    name="SMS-NTI: Sisreg Banco Producao",
    state_handlers=[handle_flow_state_change]
) as sisreg_bp_flow:
    
    ENVIRONMENT = Parameter("environment", default="dev")
    data_especifica = Parameter("data_especifica", default=None)
    data_inicial = Parameter("data_inicial", default=None)
    data_final = Parameter("data_final", default=None)
    status_especifico = Parameter("status_especifico", default=None)
    timeout_login = Parameter("timeout_login", default=30)
    timeout_consulta = Parameter("timeout_consulta", default=180)
    tempo_espera_sisreg = Parameter("tempo_espera_sisreg", default=8.5)
    limite_requisicoes_sessao = Parameter("limite_requisicoes_sessao", default=15)
    max_tentativas_reextra = Parameter("max_tentativas_reextra", default=100)

    usuario_infisical = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_VITACARE_USERNAME,
        environment=ENVIRONMENT,
    )
    senha_infisical = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_VITACARE_PASSWORD,
        environment=ENVIRONMENT,
    )

    roteiro = gerar_roteiro_extracao(
        data_especifica=data_especifica,
        data_inicial=data_inicial,
        data_final=data_final,
        status_especifico=status_especifico
    )

    resultados_fase1 = extrair_fase_principal(
        usuario=usuario_infisical, 
        senha=senha_infisical, 
        roteiro=roteiro,
        timeout_login=timeout_login,
        timeout_consulta=timeout_consulta,
        tempo_espera=tempo_espera_sisreg,
        limite_requisicoes=limite_requisicoes_sessao
    )
    
    dados_finais = extrair_fase_reextracao(
        usuario=usuario_infisical, 
        senha=senha_infisical, 
        resultados_fase1=resultados_fase1,
        timeout_login=timeout_login,
        timeout_consulta=timeout_consulta,
        tempo_espera=tempo_espera_sisreg,
        max_tentativas=max_tentativas_reextra
    )

    salvar_resultados(
        dados_extraidos=dados_finais,
        dataset_id=DATASET_ID_BRUTO,
        table_id=TABLE_ID_BP
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


    