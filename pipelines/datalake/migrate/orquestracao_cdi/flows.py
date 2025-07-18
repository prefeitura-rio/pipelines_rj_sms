# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from datetime import timedelta

from prefect import Parameter, unmapped
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
    create_dbt_params_dict,
    create_params_dict,
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
from pipelines.utils.tasks import get_project_name_from_prefect_environment

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
    #                 ┌(5) ?────┐
    #                 │  Email  │
    #                 └─────────┘      [via asciiflow.com]

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DATE = Parameter("date", default=None)
    # Precisamos usar `authenticated_task()`s, e o Prefect transforma dict()s em
    # pseudo-tasks, executadas imediatamente -- então passar um dict() como parâmetro
    # a uma task diretamente dá erro 403 com o Google Cloud. É necessário uma
    # `authenticated_task()` que retorna um dict(), que então pode ser usado diretamente
    do_params = create_params_dict(environment=ENVIRONMENT, date=DATE)

    project_name = get_project_name_from_prefect_environment()
    current_flow_run_labels = get_current_flow_labels()

    ## (1) DOU
    dou_flow_run = create_flow_run(
        flow_name="DataLake - Extração e Carga de Dados - Diário Oficial da União",
        project_name=project_name,
        parameters=do_params,
        labels=current_flow_run_labels,
    )

    ## (2) DO-RJ
    dorj_flow_run = create_flow_run(
        flow_name="DataLake - Extração e Carga de Dados - Diário Oficial Municipal",
        project_name=project_name,
        parameters=do_params,
        labels=current_flow_run_labels,
    )

    ## Agrega (1) e (2)
    wait_dos = wait_for_flow_run.map(
        # flow_run_id=[dorj_flow_run],
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

    ## (5) Email


flow_orquestracao_cdi.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_orquestracao_cdi.executor = LocalDaskExecutor(num_workers=1)
flow_orquestracao_cdi.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="4Gi",
    memory_limit="4Gi",
)

# FIXME
# flow_orquestracao_cdi.schedule = schedules
