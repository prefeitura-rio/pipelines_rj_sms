# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.infisical import inject_all_secrets
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.prefect import cancel_flow_run, get_prefect_project_id, run_query


@task
def force_secrets_injection(environment: str):
    inject_all_secrets(environment=environment)
    return


@task
def detect_running_flows(environment: str) -> pd.DataFrame:
    """
    Detects and retrieves information about long-running flow runs for a specific project.
    This function queries a GraphQL API to fetch flow runs that are currently in the "Running"
    state for a given project. The results are then processed into a pandas DataFrame with
    relevant details.
    Returns:
        pd.DataFrame: A DataFrame containing details of the long-running flow runs, including:
            - id: The ID of the flow run.
            - name: The name of the flow run.
            - state: The current state of the flow run.
            - labels: Labels associated with the flow run.
            - scheduled_start_time: The scheduled start time of the flow run.
            - version: The version of the flow run.
            - flow_name: The name of the flow.
            - flow_id: The ID of the flow.
    """
    query = """
        query UpcomingFlowRuns($projectId: uuid) {
        flow_run(
            where: {flow: {project_id: {_eq: $projectId}}, state: {_in: ["Running", "Submitted"]}}
            order_by: [{scheduled_start_time: asc}, {flow: {name: asc}}]
        ) {
            id
            name
            state
            labels
            scheduled_start_time
            version
            flow {
                id
                name
            }
            __typename
        }
    }
    """
    data = run_query.run(
        query=query,
        variables={"projectId": get_prefect_project_id.run(environment)},
    )
    if len(data["data"]["flow_run"]) == 0:
        return None

    result = pd.DataFrame(data["data"]["flow_run"])

    # Data processing
    result["flow_name"] = result["flow"].apply(lambda x: x["name"])
    result["flow_id"] = result["flow"].apply(lambda x: x["id"])
    result["composed_name"] = result["flow_name"] + " -> " + result["name"]
    result["scheduled_start_time"] = pd.to_datetime(result["scheduled_start_time"], format='mixed')  # noqa
    result["scheduled_start_time"] = result["scheduled_start_time"].dt.tz_convert(
        "America/Sao_Paulo"
    )
    result["beginning_datetime"] = result["scheduled_start_time"].apply(
        lambda x: x.strftime("%d/%m/%Y %H:%M:%S")
    )
    result["duration_minutes"] = result["scheduled_start_time"].apply(
        lambda x: (pd.Timestamp.now(tz="America/Sao_Paulo") - x).total_seconds() / 60
    )
    result["flow_run_url"] = result["id"].apply(
        lambda x: f"https://pipelines.dados.rio/flow-run/{x}"
    )

    # Classification Type
    def classify(duration):
        if duration > 24:
            return "long"
        elif duration > 12:
            return "warning"
        else:
            return "normal"

    result["classification_type"] = result["duration_minutes"].apply(classify)

    #  Classification Emoji
    def classify_emoji(duration):
        if duration > 24:
            return "☠️"
        elif duration > 12:
            return "⚠️"
        else:
            return ""

    result["classification_emoji"] = result["duration_minutes"].apply(classify_emoji)

    result.drop(columns="flow", inplace=True)

    return result


@task
def report_flows(running_flows: pd.DataFrame):
    """
    Logs information about long-running flow runs.

    This function iterates over each row in the provided DataFrame and logs a message
    containing the flow run ID, flow name, and the scheduled start time.

    Args:
        long_running_flows (pd.DataFrame): A DataFrame containing information about long-running
            flow runs. Expected columns are:
            - 'id': The unique identifier of the flow run.
            - 'flow_name': The name of the flow.
            - 'scheduled_start_time': The scheduled start time of the flow run.
    """
    if running_flows is None:
        log("No long running flows detected.")
        return

    running_flows.sort_values(by="scheduled_start_time", ascending=True, inplace=True)

    content = [f"Detectamos {running_flows.shape[0]} flow runs:"]
    for _, flow_run in running_flows.iterrows():
        name = flow_run["composed_name"]
        url = flow_run["flow_run_url"]
        duration_minutes = flow_run["duration_minutes"]
        beginning_datetime = flow_run["beginning_datetime"]
        emoji = flow_run["classification_emoji"]
        content.append(
            f"- {emoji} [**{name}**]({url}) :: {duration_minutes:.1f} minutos desde {beginning_datetime}."  # noqa
        )

    full_message = "\n".join(content)
    log(full_message)
    send_message(title="⌛ Execuções em Andamento", message=full_message, monitor_slug="warning")
    return


@task
def cancel_flows(running_flows: pd.DataFrame):
    """
    Cancels a list of long-running flow runs.
    This function takes a list of dictionaries, where each dictionary represents
    a flow run with at least an 'id' key. It attempts to cancel each flow run
    and logs the progress and results.
    Args:
        running_flows (list[dict]): A list of dictionaries, each containing
                                         information about a long-running flow run.
                                         Each dictionary must have an 'id' key.
    Returns:
        None
    """

    if running_flows is None:
        log("No running flows detected.")
        return

    long_running_flows = running_flows[running_flows["classification_type"] == "long"]
    if long_running_flows.shape[0] == 0:
        log("No long running flows detected.")
        return

    log(f"Cancelling {long_running_flows.shape[0]} long running flows.")

    full_message = [f"São {long_running_flows.shape[0]} execuções longas em cancelamento:"]

    for _, flow_run in long_running_flows.iterrows():
        success = cancel_flow_run.run(flow_run["id"])
        success_emoji = "✅" if success else "❌"
        message = f"- [**{flow_run['composed_name']}**]({flow_run['flow_run_url']}) de {flow_run['duration_minutes']:.1f} minutos :: Sucesso {success_emoji}"  # noqa
        full_message.append(message)
        log(message)

    send_message(
        title="☠️ Execuções Canceladas", message="\n".join(full_message), monitor_slug="warning"
    )
    return
