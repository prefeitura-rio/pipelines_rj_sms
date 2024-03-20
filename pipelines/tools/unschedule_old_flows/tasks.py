# -*- coding: utf-8 -*-
import traceback
from datetime import datetime

from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


@task
def query_active_flow_names(environment="dev", prefect_client=None):
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
    project_name = "staging" if environment == "dev" else "production"

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

    return active_flows


@task
def query_not_active_flows(flows, environment="dev", prefect_client=None):
    """
    Queries the graphql API for scheduled flow_runs of
    archived versions of <flow_name>

    Args:
        flow_name (str): flow name
    """
    project_name = "staging" if environment == "dev" else "production"

    flow_name, last_version = flows
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
    archived_flows = []
    response = prefect_client.graphql(query=query, variables=variables)["data"]

    for flow in response["flow"]:
        if flow["flow_runs"]:
            try:
                archived_flows.append(
                    {
                        "id": flow["id"],
                        "name": flow["name"],
                        "version": flow["version"],
                        "count": len(flow["flow_runs"]),
                    }
                )
                log(f"Detected Archived Flow: {flow['name']} - {flow['version']}")
            except Exception:
                log(flow)

    return archived_flows


@task
def get_prefect_client():
    return Client()


@task
def cancel_flows(flows, prefect_client: Client = None) -> None:
    """
    Cancels a flow run from the API.
    """
    if not flows:
        return
    log(">>>>>>>>>> Cancelling flows")

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
    cancelled_flows = []
    for flow in flows:
        try:
            response = prefect_client.graphql(query=query, variables=dict(flow_id=flow["id"]))
            log(response)
            log(f">>>>>>>>>> Flow run {flow['id']} arquivada")
            cancelled_flows.append(flow)
        except Exception:
            log(traceback.format_exc())
            log(f"Flow {flow['id']} could not be cancelled")

    log(f"# of Cancelled flows: {len(cancelled_flows)}")
