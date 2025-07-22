# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
DBT flows
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.transform.gemini.pacientes_restritos.constants import (
    constants as hci_pacientes_restritos_constants,
)
from pipelines.datalake.transform.gemini.pacientes_restritos.schedules import (
    hci_pacientes_restritos_daily_update_schedule,
)
from pipelines.datalake.transform.gemini.pacientes_restritos.tasks import (
    get_result_gemini,
    parse_result_dataframe,
    reduce_raw_column,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_secret_key,
    query_table_from_bigquery,
    rename_current_flow_run,
    upload_df_to_datalake,
)

with Flow(
    name="HCI - Transformação de dados - Detecção de pacientes restritos",
    state_handlers=[handle_flow_state_change],
    owners=[constants.VITORIA_ID.value],
) as hci_pacientes_restritos:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    QUERY = Parameter("query", default=hci_pacientes_restritos_constants.QUERY.value)

    #####################################
    # Set environment
    ####################################

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(environment=ENVIRONMENT)

    ####################################
    # Tasks section #1 - Getting encounters with keywords
    #####################################
    query_results = query_table_from_bigquery(
        sql_query=QUERY,
        env=ENVIRONMENT,
    )

    ####################################
    # Tasks section #2 - Prepare data and use Gemini
    #####################################

    motivo_atendimento_reduced, id_atendimento, cpf = reduce_raw_column(df=query_results)

    gemini_key = get_secret_key(
        secret_path=hci_pacientes_restritos_constants.INFISICAL_PATH.value,
        secret_name=hci_pacientes_restritos_constants.INFISICAL_API_KEY.value,
        environment=ENVIRONMENT,
    )
    list_result = get_result_gemini.map(
        atendimento=motivo_atendimento_reduced,
        id_atendimento=id_atendimento,
        cpf=cpf,
        gemini_key=unmapped(gemini_key),
    )
    df_result = parse_result_dataframe(list_result=list_result)

    #     ####################################
    #     # Tasks section #3 - Load data to BQ
    #     #####################################

    upload_to_datalake_task = upload_df_to_datalake(
        df=df_result,
        table_id=hci_pacientes_restritos_constants.TABLE_ID.value,
        dataset_id=hci_pacientes_restritos_constants.DATASET_ID.value,
        partition_column="_extracted_at",
        if_exists="append",
        if_storage_data_exists="append",
        source_format="csv",
        csv_delimiter=",",
    )


# Storage and run configs
hci_pacientes_restritos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
hci_pacientes_restritos.executor = LocalDaskExecutor(num_workers=10)
hci_pacientes_restritos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

hci_pacientes_restritos.schedule = hci_pacientes_restritos_daily_update_schedule
