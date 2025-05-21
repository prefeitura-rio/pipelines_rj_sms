# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
DBT flows
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants
from pipelines.prontuarios.std.deteccao_restricoes.constants import (
    constants as hci_pacientes_restritos_constants,
) 
from pipelines.prontuarios.std.deteccao_restricoes.schedules import hci_pacientes_restritos_daily_update_schedule
from pipelines.prontuarios.std.deteccao_restricoes.tasks import (
    get_result_gemini,
    reduce_raw_column,
    parse_result_dataframe
)

from pipelines.utils.tasks import (
    create_folders,
    query_table_from_bigquery,
    rename_current_flow_run,
    upload_to_datalake,
    saving_results,
)

with Flow(name="HCI - Detecção de pacientes restritos") as hci_pacientes_restritos:
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
        sql_query = QUERY,
        env=ENVIRONMENT,
    )

    ####################################
    # Tasks section #2 - Prepare data and use Gemini
    #####################################

    motivo_atendimento_reduced, id_atendimento, cpf = reduce_raw_column(df=query_results)
    list_result = get_result_gemini.map(atendimento=motivo_atendimento_reduced, 
                                        id_atendimento=id_atendimento,
                                        cpf=cpf,
                                        env=ENVIRONMENT)
    df_result = parse_result_dataframe(list_result=list_result)

    ####################################
    # Tasks section #3 - Load data to BQ
    #####################################
    create_folders_task = create_folders()
    path = saving_results(
        result=df_result,
        file_folder=create_folders_task["raw"],
    )

    upload_to_datalake_task = upload_to_datalake(
        input_path=path,
        dataset_id=hci_pacientes_restritos_constants.TABLE_ID,
        table_id=hci_pacientes_restritos_constants.DATASET_ID,
        dump_mode="append",
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
