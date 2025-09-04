# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

##
from pipelines.datalake.migrate.orquestracao_cdi.constants import (
    constants as flow_constants,
)
from pipelines.datalake.migrate.orquestracao_cdi.schedules import schedules
from pipelines.datalake.migrate.orquestracao_cdi.tasks import (
    build_email,
    create_dbt_params_dict,
    create_dorj_params_dict,
    create_dou_params_dict,
    create_tcm_params_dict,
    fetch_tcm_cases,
    get_email_recipients,
    get_todays_tcm_from_gcs,
    send_email,
)

##
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_project_name_from_prefect_environment,
    get_secret_key,
)

##

with Flow(
    name="DataLake - Agendador de Flows - CDI",
    state_handlers=[handle_flow_state_change],
    owners=[constants.AVELLAR_ID.value],
) as flow_orquestracao_cdi:

    #  ┌(1) Flow────────┐     ┌(2) Flow──────────┐
    #  │  Extração DOU  ├──┬──┼  Extração DO-RJ  │
    #  └────────────────┘  │  └──────────────────┘
    #                      ▼
    #          ┌(3) dbt─────────────────┐
    #          │  Filtro de relevância  │
    #          └───────────┬────────────┘
    #                      └───────►┌(4) Flow───────────┐
    #                      ┌────────┼  Extração do TCM  │
    #                      ▼        └───────────────────┘
    #                 ┌(5) Task─┐
    #                 │  Email  │
    #                 └─────────┘      [via asciiflow.com]

    ENVIRONMENT = Parameter("environment", default="prod", required=True)
    DATE = Parameter("date", default=None)
    SKIP_TO_EMAIL = Parameter("skip_to_email", default=False)
    OVERRIDE_RECIPIENTS = Parameter("override_recipients", default=None)

    ## Pipeline completo
    with case(SKIP_TO_EMAIL, False):
        # Precisamos usar `authenticated_task()`s, e o Prefect transforma dict()s em
        # pseudo-tasks, executadas imediatamente -- então passar um dict() como parâmetro
        # a uma task diretamente dá erro 403 com o Google Cloud. É necessário uma
        # `authenticated_task()` que retorna um dict(), que então pode ser usado diretamente
        dorj_params = create_dorj_params_dict(environment=ENVIRONMENT, date=DATE)
        dou_params = create_dou_params_dict(environment=ENVIRONMENT, date=DATE)

        project_name = get_project_name_from_prefect_environment()
        current_flow_run_labels = get_current_flow_labels()

        ## (1) DOU
        dou_flow_run = create_flow_run(
            flow_name="DataLake - Extração e Carga de Dados - Diário Oficial da União (API)",
            project_name=project_name,
            parameters=dou_params,
            labels=current_flow_run_labels,
        )

        ## (2) DO-RJ
        dorj_flow_run = create_flow_run(
            flow_name="DataLake - Extração e Carga de Dados - Diário Oficial Municipal",
            project_name=project_name,
            parameters=dorj_params,
            labels=current_flow_run_labels,
        )

        ## Agrega (1) e (2)
        wait_dos = wait_for_flow_run.map(
            flow_run_id=[dou_flow_run, dorj_flow_run],
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
            max_duration=unmapped(timedelta(minutes=60)),
        )

        ## (3) dbt
        dbt_params = create_dbt_params_dict(environment=ENVIRONMENT)
        dbt_flow_run = create_flow_run(
            flow_name="DataLake - Transformação - DBT",
            project_name=project_name,
            parameters=dbt_params,
            labels=current_flow_run_labels,
            upstream_tasks=[wait_dos],
        )

        wait_dbt = wait_for_flow_run(
            flow_run_id=dbt_flow_run,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
            max_duration=timedelta(minutes=20),
        )

        ## (4) TCM
        # Espera DBT terminar, pega casos da tabela
        tcm_cases = fetch_tcm_cases(
            environment=ENVIRONMENT,
            date=DATE,
            upstream_tasks=[wait_dbt],
        )
        # Cria um dict() de parâmetros para cada caso da tabela
        tcm_params = create_tcm_params_dict.map(
            case_id=tcm_cases,
            environment=unmapped(ENVIRONMENT),
        )

        tcm_flow_runs = create_flow_run.map(
            flow_name=unmapped(
                "DataLake - Extração e Carga de Dados - Tribunal de Contas do Município"
            ),
            project_name=unmapped(project_name),
            parameters=tcm_params,
            labels=unmapped(current_flow_run_labels),
        )
        wait_tcm = wait_for_flow_run.map(
            flow_run_id=tcm_flow_runs,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(False),
            max_duration=unmapped(timedelta(minutes=20)),
        )

        ## (5) Email
        URL = get_secret_key(
            secret_path=flow_constants.EMAIL_PATH.value,
            secret_name=flow_constants.EMAIL_URL.value,
            environment=ENVIRONMENT,
        )
        TOKEN = get_secret_key(
            secret_path=flow_constants.EMAIL_PATH.value,
            secret_name=flow_constants.EMAIL_TOKEN.value,
            environment=ENVIRONMENT,
        )
        df = get_todays_tcm_from_gcs(
            environment=ENVIRONMENT, skipped=False, upstream_tasks=[wait_tcm]
        )
        (edition, error, message) = build_email(environment=ENVIRONMENT, date=DATE, tcm_df=df)
        recipients = get_email_recipients(environment=ENVIRONMENT, recipients=OVERRIDE_RECIPIENTS, error=error)
        send_email(date=DATE, api_base_url=URL, token=TOKEN, recipients=recipients, edition=edition, message=message)

    ## Somente envio de email
    with case(SKIP_TO_EMAIL, True):
        URL = get_secret_key(
            secret_path=flow_constants.EMAIL_PATH.value,
            secret_name=flow_constants.EMAIL_URL.value,
            environment=ENVIRONMENT,
        )
        TOKEN = get_secret_key(
            secret_path=flow_constants.EMAIL_PATH.value,
            secret_name=flow_constants.EMAIL_TOKEN.value,
            environment=ENVIRONMENT,
        )
        df = get_todays_tcm_from_gcs(environment=ENVIRONMENT, skipped=True)
        (edition, error, message) = build_email(environment=ENVIRONMENT, date=DATE, tcm_df=df)
        recipients = get_email_recipients(environment=ENVIRONMENT, recipients=OVERRIDE_RECIPIENTS, error=error)
        send_email(date=DATE, api_base_url=URL, token=TOKEN, recipients=recipients, edition=edition, message=message)


flow_orquestracao_cdi.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_orquestracao_cdi.executor = LocalDaskExecutor(num_workers=1)
flow_orquestracao_cdi.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)

flow_orquestracao_cdi.schedule = schedules
