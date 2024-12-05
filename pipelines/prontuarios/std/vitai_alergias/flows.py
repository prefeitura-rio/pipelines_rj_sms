# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for Vitai Allergies Standardization
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.std.vitai_alergias.constants import constants as vitai_alergias_constants
from pipelines.prontuarios.std.vitai_alergias.schedules import vitai_alergias_daily_update_schedule
from pipelines.prontuarios.std.vitai_alergias.tasks import (
    create_allergie_list,
    get_similar_allergie_levenshtein,
    get_similar_allergie_gemini,
    validade_medlm
)
from pipelines.utils.tasks import (
    rename_current_flow_run,
    upload_to_datalake,
    load_file_from_bigquery,
    get_secret_key
)

####################################
# Daily Routine Flow
####################################
with Flow(
    name="Prontuário (VITAI) - Padronização de Alergias",
) as sms_prontuarios_vitai_alergias_padronizacao:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    #####################################
    # Set environment
    ####################################
    vitai_allergies = load_file_from_bigquery(
        project_name=vitai_alergias_constants.PROJECT.value[ENVIRONMENT],
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_vitai",
        environment=ENVIRONMENT
    )
    reference_allergies = load_file_from_bigquery(
        project_name=vitai_alergias_constants.PROJECT.value[ENVIRONMENT],
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_referencia",
        environment=ENVIRONMENT
    )
    key = get_secret_key(
        secret_name=vitai_alergias_constants.INFISICAL_API_KEY.value, 
        secret_path=vitai_alergias_constants.INFISICAL_PATH.value, 
        environment=ENVIRONMENT
    )
    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT, unidade="VITAI"
        )
    ####################################
    # Task Section #2 - Standardization
    ####################################

    vitai_allergies_list = create_allergie_list(vitai_allergies)

    std_allergies, no_std_allergies = get_similar_allergie_levenshtein(
        allergies_dataset_reference=reference_allergies, 
        allergie_list=vitai_allergies_list,
        threshold=0.85
    )
    gemini_result = get_similar_allergie_gemini(
        allergies_list=no_std_allergies,
        key=key
    )
    result = validade_medlm(
        gemini_result=gemini_result, 
        levenshtein_result=std_allergies
    )

    ####################################
    # Task Section #3 - Loading data
    ####################################
    upload_to_datalake(
        input_path=result,
        dataset_id="intermediario_...",
        table_id="vitai_padronizacao",
        dump_mode="append"
    )


sms_prontuarios_vitai_alergias_padronizacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_vitai_alergias_padronizacao.executor = LocalDaskExecutor(num_workers=1)
sms_prontuarios_vitai_alergias_padronizacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="5Gi",
    memory_limit="5Gi",
)

# sms_prontuarios_vitai_alergias_padronizacao.schedule = smsrio_daily_update_schedule
