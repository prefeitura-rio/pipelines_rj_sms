# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
DBT flows
"""
from prefect import Parameter
from prefect.run_configs import VertexRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tmp.tasks import get_ip_geolocation, get_my_ip, log_ip_and_info

with Flow(name="Vertex Agent Example") as tmp__vertex_agent_example__flow:
    environment = Parameter("environment", default="dev")
    ip = get_my_ip(environment=environment)  # Just to keep environment at use
    info = get_ip_geolocation(ip=ip)
    log_ip_and_info(ip=ip, info=info)

# Storage and run configs
tmp__vertex_agent_example__flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
tmp__vertex_agent_example__flow.run_config = VertexRun(
    image=constants.DOCKER_VERTEX_IMAGE.value,
    labels=[
        constants.RJ_SMS_VERTEX_AGENT_LABEL.value,
    ],
    # https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": constants.INFISICAL_TOKEN.value,
    },
)