# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log

import pipelines.utils.monitor as monitor
from pipelines.utils.credential_injector import authenticated_task as task


@task
def query_active_flow_names(environment="staging", prefect_client=None):
    """
    Queries the active flow names and versions from the Prefect server.

    Args:
        environment (str, optional): The environment to query the active flows for.
        prefect_client (prefect.Client, optional): The Prefect client to use for the query.

    Returns:
        list: A list of tuples containing the active flow names and versions.

    Raises:
        prefect.utilities.exceptions.ClientError: If an error occurs during the query.

    """
    project_name = "production" if environment == "prod" else environment

    query = """
        query ($offset: Int, $project_name: String){
            flow(
                where: {
                    archived: {_eq: false},
                    project: {name:{_eq:$project_name}}
                }
                offset: $offset
            ){
                name
                version
            }
        }
    """
    if not prefect_client:
        prefect_client = Client()
    variables = {"offset": 0, "project_name": project_name}

    response = prefect_client.graphql(query=query, variables=variables)["data"]
    active_flows = []
    for flow in response["flow"]:
        log(f"Detected Active Flow: {flow['name']}")
        active_flows.append((flow["name"], flow["version"]))

    active_flows = list(set(active_flows))
    log(f"Total Active Flow: {len(active_flows)}")

    return active_flows


@task
def query_not_active_flows(flow_data, environment="staging", prefect_client=None):
    """
    Queries the graphql API for scheduled flow_runs of
    archived versions of <flow_name>

    Args:
        flow_name (str): flow name
    """
    project_name = "production" if environment == "prod" else environment
    flow_name, last_version = flow_data

    log(f"Querying for archived flow runs of {flow_name} in {project_name}.")
    now = datetime.now().isoformat()
    query = """
        query($flow_name: String, $last_version: Int, $now: timestamptz!, $offset: Int, $project_name: String){ # noqa
            flow(
                where:{
                    name: {_eq:$flow_name},
                    version: {_lt:$last_version}
                    project: {name:{_eq:$project_name}}
                }
                offset: $offset
                order_by: {version:desc}
            ){
                id
                name
                version
                flow_runs(
                    where:{
                        scheduled_start_time: {_gte: $now},
                        state: {_nin: ["Cancelled"]}
                    }
                    order_by: {version:desc}
                ){
                    id
                    scheduled_start_time
                }
            }
        }
    """
    if not prefect_client:
        prefect_client = Client()

    variables = {
        "flow_name": flow_name,
        "last_version": last_version,
        "now": now,
        "offset": 0,
        "project_name": project_name,
    }
    response = prefect_client.graphql(query=query, variables=variables)["data"]
    log(f"Response Length: {len(response['flow'])}")

    flow_versions_to_cancel = []
    log("WARNING: The following (Flow, Version) have scheduled runs that must be cancelled.")
    for flow in response["flow"]:
        flow_runs = flow["flow_runs"]

        # Se não houver flow_runs futuras, não é necessário cancelar
        if len(flow_runs) == 0:
            continue

        flow_versions_to_cancel.append(
            {"id": flow["id"], "name": flow["name"], "version": flow["version"]}
        )
        log(f"({flow['name']}, {flow['version']}) - Last Version = {last_version}")

    return flow_versions_to_cancel


@task
def get_prefect_client():
    """
    Returns a Prefect client object.

    :return: Prefect client object.
    """
    return Client()


@task
def cancel_flows(flow_versions_to_cancel: list, prefect_client: Client = None) -> None:
    """
    Cancels a flow run from the API.
    """

    if not prefect_client:
        prefect_client = Client()

    query = """
        mutation($flow_id: UUID!) {
            archive_flow (
                input: {
                    flow_id: $flow_id
                }
            ) {
                success
            }
        }
    """
    reports = []
    for flow_version in flow_versions_to_cancel:
        response = prefect_client.graphql(query=query, variables=dict(flow_id=flow_version["id"]))
        report = f"- Flow {flow_version['name']} de versão {flow_version['id']} arquivado com status: {response}"  # noqa
        log(report)

        reports.append(report)

    monitor.send_message(
        title="Resultado da varredura",
        message="\n".join(reports),
        username="Desagendador de Flows Antigos",
    )
