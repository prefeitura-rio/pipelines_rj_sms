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
from pipelines.prontuarios.std.alergias_vitai.constants import (
    constants as vitai_alergias_constants,
)
from pipelines.prontuarios.std.alergias_vitai.schedules import (
    vitai_alergias_daily_update_schedule,
)
from pipelines.prontuarios.std.alergias_vitai.tasks import (
    create_allergie_list,
    get_api_token,
    get_similar_allergie_gemini,
    get_similar_allergie_levenshtein,
    saving_results,
)
from pipelines.utils.tasks import (
    load_file_from_bigquery,
    rename_current_flow_run,
    upload_to_datalake,
    create_folders,
)

from pipelines.utils.tasks import get_secret_key

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
        project_name=vitai_alergias_constants.PROJECT.value["dev"],
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_vitai",
        environment=ENVIRONMENT,
    )
    reference_allergies = load_file_from_bigquery(
        project_name=vitai_alergias_constants.PROJECT.value["dev"],
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_referencia",
        environment=ENVIRONMENT,
    )
    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(environment=ENVIRONMENT, unidade="VITAI")
    ####################################
    # Task Section #2 - Standardization
    ####################################

    vitai_allergies_list = create_allergie_list(dataframe_allergies_vitai=vitai_allergies)

    std_allergies, no_std_allergies = get_similar_allergie_levenshtein(
        allergies_dataset_reference=reference_allergies,
        allergie_list=vitai_allergies_list,
        threshold=0.85,
    )
    api_token, api_url = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitai_alergias_constants.INFISICAL_PATH.value,
        infisical_api_url=vitai_alergias_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitai_alergias_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitai_alergias_constants.INFISICAL_API_PASSWORD.value
    )
    gemini_result = get_similar_allergie_gemini(
        allergies_list=no_std_allergies,
        api_url=api_url,
        api_token=api_token
    )
    create_folders_task = create_folders()
    path = saving_results(
        gemini_result=gemini_result, 
        levenshtein_result=std_allergies,
        file_folder=create_folders_task["raw"],
    )

    ####################################
    # Task Section #3 - Loading data
    ####################################
    upload_to_datalake_task = upload_to_datalake(
        input_path=path,
        dataset_id="intermediario_historico_clinico",
        table_id="vitai_padronizacao",
        dump_mode="append",
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