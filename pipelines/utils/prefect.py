# -*- coding: utf-8 -*-
import requests
from prefeitura_rio.pipelines_utils.env import getenv_or_action

from pipelines.constants import constants
from pipelines.utils.logger import log


def get_prefect_token() -> str:
    """
    Authenticates with the Prefect API and retrieves an authentication token.
    This function sends a POST request to the Prefect API's login endpoint using
    the username and password obtained from environment variables.
    If the authentication is successful, it returns the authentication token.
    Returns:
        str: The authentication token if the request is successful.
    Raises:
        requests.exceptions.RequestException: If the request fails or the response
        status code is not 200.
    """
    response = requests.post(
        url=f"{getenv_or_action('PREFECT_API_URL')}auth/login/",
        json={
            "username": getenv_or_action("PREFECT_API_USERNAME"),
            "password": getenv_or_action("PREFECT_API_PASSWORD"),
        },
        timeout=180,
    )

    if response.status_code == 200:
        body = response.json()
        token = body["token"]
        log("Successfully authenticated with Prefect API.")
        return token
    else:
        raise requests.exceptions.RequestException(
            f"Failed to authenticate with Prefect API. Status code: {response.status_code}"
        )


def run_query(
    query,
    variables: dict = None,
    token: str = None,
):
    """
    Perform a GraphQL query and return results.
    """
    variables = variables or {}
    token = token or get_prefect_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Prefect-Tenant-Id": getenv_or_action("PREFECT_API_TENANT_ID"),
    }
    r = requests.post(
        url=f"{getenv_or_action('PREFECT_API_URL')}proxy/",
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=180,
    )
    if r.status_code == 200:
        log("Successfully ran query.")
        return r.json()
    else:
        log(f"Failed to run query: [{r.status_code}] {r.text}")
        return None


def cancel_flow_run(flow_run_id: str, token: str = None) -> bool:
    """
    Cancels a Prefect flow run given its ID.
    Args:
        flow_run_id (str): The unique identifier of the flow run to be cancelled.
        token (str, optional): An optional authentication token for the Prefect API.
    Returns:
        bool: True if the flow run was successfully cancelled, False otherwise.
    """
    mutation = """
        mutation CancelFlowRun($flow_run_id: UUID!) {
            cancel_flow_run(input: {flow_run_id: $flow_run_id}) {
                state
            }
        }
    """
    variables = {"flow_run_id": flow_run_id}
    data: dict = run_query(query=mutation, variables=variables, token=token)
    return data["data"]["cancel_flow_run"]["state"] in ["Cancelled", "Cancelling"]


def archive_flow_run(flow_id: str, token: str = None) -> bool:
    """
    Archives a flow run in the Prefect system.
    Args:
        flow_id (str): The unique identifier of the flow to be archived.
        token (str, optional): The authentication token for the Prefect API. Defaults to None.
    Returns:
        bool: True if the flow was successfully archived, False otherwise.
    flow_id: str,
    token: str = None
    """
    mutation = """
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
    variables = {"flow_id": flow_id}
    data: dict = run_query(query=mutation, variables=variables, token=token)
    return data["data"]["archive_flow"]["success"]


def get_prefect_project_id(environment: str = "staging") -> str:
    """
    Retrieves the Prefect project ID for the given environment.
    Args:
        environment (str, optional): The environment for which to retrieve the Prefect project ID.
    Returns:
        str: The Prefect project ID for the given environment.
    """
    project_name = constants.PROJECT_NAME.value.get(environment, "staging")
    query = """
        query($project_name: String!) {
            project(where: {name: {_eq: $project_name}}) {
                id
            }
        }
    """
    variables = {"project_name": project_name}
    data: dict = run_query(query=query, variables=variables)
    return data["data"]["project"][0]["id"]
