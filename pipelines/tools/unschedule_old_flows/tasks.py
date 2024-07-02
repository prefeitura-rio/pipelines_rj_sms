# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.client import Client

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def query_non_archived_flows(environment="staging"):
    """
    Queries the non-archived flows from the Prefect API for a given environment.

    Args:
        environment (str, optional): The environment to query the flows from.

    Returns:
        list: A list of unique non-archived flows in the format [(name, version)].

    Raises:
        Exception: If there is an error in the GraphQL request or response.
    """
    project_name = "production" if environment == "prod" else environment
    prefect_client = Client()
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

    # Request data from Prefect API
    request_response = prefect_client.graphql(
        query=query, variables={"offset": 0, "project_name": project_name}
    )
    data = request_response["data"]

    active_flows = [(flow["name"], flow["version"]) for flow in data["flow"]]
    log(f"Number of Non Archived Flows: {len(active_flows)}")

    unique_non_archived_flows = list(set(active_flows))
    log(f"Number of Unique Non Archived Flows: {len(unique_non_archived_flows)}")

    lines = [f"- {flow['name']}@v{flow['version']}" for flow in data["flow"]]
    message = f"Non Archived Flows in Project {project_name}:\n" + "\n".join(lines)
    log(message)

    return unique_non_archived_flows


@task
def query_archived_flow_versions_with_runs(flow_data, environment="staging"):
    """
    Queries for archived flow versions with scheduled runs.

    Args:
        flow_data (tuple): A tuple containing the flow name and the last version.
        environment (str, optional): The environment to query the flows in.

    Returns:
        list: A list of dictionaries representing the archived flow versions with scheduled runs.
            Each dictionary contains the following keys:
                - id (str): The ID of the flow.
                - name (str): The name of the flow.
                - version (int): The version of the flow.
                - invalid_runs_count (int): The number of invalid runs for the flow.
    """

    project_name = "production" if environment == "prod" else environment
    prefect_client = Client()
    flow_name, last_version = flow_data

    log(f"Querying for archived flow runs of {flow_name} < v{last_version} in {project_name}.")
    now = datetime.now().isoformat()
    query = """
        query($flow_name: String, $last_version: Int, $now: timestamptz!, $offset: Int, $project_name: String){
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

    # Request data from Prefect API
    request_response = prefect_client.graphql(
        query=query,
        variables={
            "flow_name": flow_name,
            "last_version": last_version,
            "now": now,
            "offset": 0,
            "project_name": project_name,
        },
    )

    data = request_response["data"]

    flow_versions_to_cancel = []
    for flow in data["flow"]:
        flow_runs = flow["flow_runs"]

        # Se não houver flow_runs futuras, não é necessário cancelar
        if len(flow_runs) == 0:
            continue

        flow_versions_to_cancel.append(
            {
                "id": flow["id"],
                "name": flow["name"],
                "version": flow["version"],
                "invalid_runs_count": len(flow_runs),
            }
        )

    if len(flow_versions_to_cancel) == 0:
        log(f"No archived flows with scheduled runs found for {flow_name} < v{last_version}.")
        return []

    lines = [
        f"- {flow['name']} @ v{flow['version']} ({flow['id']}) has {flow['invalid_runs_count']} invalid runs"
        for flow in flow_versions_to_cancel
    ]
    message = f"Archived Flows with Scheduled Runs in Project {project_name}:\n" + "\n".join(lines)
    log(message)

    return flow_versions_to_cancel


@task()
def report_to_discord(flows_to_archive: list[list[dict]]):
    """
    Sends a report to Discord with information about flows to be archived.

    Args:
        flows_to_archive (list[list[dict]]): A list of flows to be archived. Each flow is
            represented as a dictionary.

    Returns:
        None
    """
    reports = []
    for flow_list in flows_to_archive:
        for flow in flow_list:
            if flow["invalid_runs_count"] == 0:
                continue
            flow_title = f"{flow['name']} @ v{flow['version']}"
            flow_url = f"https://pipelines.dados.rio/flow/{flow['id']}"
            reports.append(
                f"""- [{flow_title}]({flow_url}) has {flow['invalid_runs_count']} invalid runs"""
            )

    send_message(
        title="Archived Flows with Scheduled Runs",
        message="\n".join(reports),
        monitor_slug="warning",
    )


@task
def archive_flow_versions(flow_versions_to_archive: list) -> None:
    """
    Archive flow versions from the API.
    """
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
    for flow_version in flow_versions_to_archive:
        response = prefect_client.graphql(query=query, variables=dict(flow_id=flow_version["id"]))
        report = f"- Flow {flow_version['name']} de versão {flow_version['id']} arquivado com status: {response}"  # noqa
        log(report)

        reports.append(report)
