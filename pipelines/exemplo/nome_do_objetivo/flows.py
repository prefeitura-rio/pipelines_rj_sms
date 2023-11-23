# -*- coding: utf-8 -*-
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.exemplo.nome_do_objetivo.tasks import greet

with Flow(
    name="rj-sms: Nome do objetivo - Descrição do objetivo",
) as exemplo__nome_do_objetivo__greet_flow:
    # Parameters
    name = Parameter("name", default="rj_sms")

    # Tasks
    greet_task = greet(name)

# Storage and run configs
exemplo__nome_do_objetivo__greet_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
exemplo__nome_do_objetivo__greet_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
