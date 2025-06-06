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
from pipelines.datalake.transform.gemini.alergias_vitai.constants import (
    constants as vitai_alergias_constants,
)
from pipelines.datalake.transform.gemini.alergias_vitai.schedules import (
    vitai_alergias_daily_update_schedule,
)
from pipelines.datalake.transform.gemini.alergias_vitai.tasks import (
    create_allergie_list,
    get_api_token,
    get_similar_allergie_gemini,
    get_similar_allergie_levenshtein,
    load_std_dataset,
    saving_results,
)
from pipelines.utils.tasks import (
    load_file_from_bigquery,
    rename_current_flow_run,
    upload_df_to_datalake,
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
    IS_HISTORICAL = Parameter("is_historical", default=False)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(environment=ENVIRONMENT, unidade="VITAI")

    #####################################
    # Set environment
    ####################################
    vitai_allergies = load_file_from_bigquery(
        project_name="rj-sms",
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_vitai",
        environment=ENVIRONMENT,
    )
    reference_allergies = load_file_from_bigquery(
        project_name="rj-sms",
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_referencia",
        environment=ENVIRONMENT,
    )

    std_allergies = load_std_dataset(
        project_name="rj-sms",
        dataset_name="intermediario_historico_clinico",
        table_name="alergias_vitai_padronizacao",
        environment=ENVIRONMENT,
        is_historical=IS_HISTORICAL,
    )

    ####################################
    # Task Section #2 - Standardization
    ####################################

    df_vitai_allergies, allergies_list = create_allergie_list(
        dataframe_allergies_vitai=vitai_allergies, std_allergies=std_allergies
    )

    std_allergies, no_std_allergies = get_similar_allergie_levenshtein(
        allergies_dataset_reference=reference_allergies,
        allergie_list=allergies_list,
        threshold=0.8,
    )
    api_token, api_url = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitai_alergias_constants.INFISICAL_PATH.value,
        infisical_api_url=vitai_alergias_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitai_alergias_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitai_alergias_constants.INFISICAL_API_PASSWORD.value,
    )
    gemini_result = get_similar_allergie_gemini(
        allergies_list=no_std_allergies, api_url=api_url, api_token=api_token
    )

    raw_allergies = saving_results(
        raw_allergies=df_vitai_allergies,
        gemini_result=gemini_result,
        levenshtein_result=std_allergies,
    )

    # ####################################
    # # Task Section #3 - Loading data
    # ####################################
    upload_to_datalake_task = upload_df_to_datalake(
        df=raw_allergies,
        dataset_id=vitai_alergias_constants.DATASET_ID.value,
        table_id=vitai_alergias_constants.TABLE_ID.value,
        partition_column="_extracted_at",
        if_exists="append",
        if_storage_data_exists="append",
        source_format="csv",
        csv_delimiter=",",
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

sms_prontuarios_vitai_alergias_padronizacao.schedule = vitai_alergias_daily_update_schedule
