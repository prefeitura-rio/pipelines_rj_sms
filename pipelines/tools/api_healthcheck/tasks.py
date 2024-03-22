# -*- coding: utf-8 -*-
import datetime

import prefect
import requests
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
import pipelines.utils.monitor as monitor


@task
def get_api_url(api_url_table):
    return api_url_table.to_dict("records")


@task
def check_api_health(api_info: dict):
    logger = prefect.context.get("logger")
    logger.info(f"Checking API health for {api_info['url']}")

    try:
        response = requests.get(api_info["url"], timeout=90)
    except requests.Timeout:
        logger.error(f"API {api_info['url']} -> Timeout")
        duration = None
        status_code = "408"
        is_healthy = False
    else:
        duration = response.elapsed.total_seconds()
        status_code = str(response.status_code)
        is_healthy = status_code == str(api_info["expected_status_code"])

    logger.info(f"API Health {is_healthy}; Duration {duration}s; Status {status_code}")

    return {
        "api_url": api_info["url"],
        "is_healthy": is_healthy,
        "duration": duration,
        "status_code": status_code,
        "moment": datetime.datetime.now().isoformat(),
    }


@task
def insert_results(rows_to_insert: list[dict]):
    logger = prefect.context.get("logger")

    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")
    table = bq_client.get_table("rj-sms.gerenciamento.api_health_check")

    errors = bq_client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        logger.info(f"Inserted {len(rows_to_insert)} rows")
    else:
        logger.error(f"Errors: {errors}")
        raise ValueError(f"Errors: {errors}")

@task
def create_discord_alert(rows_to_insert: list[dict]) -> None:
    """
    Sends a Discord alert message based on the health status of APIs.

    Args:
        rows_to_insert (list[dict]): A list of dictionaries representing the API health status.

    Returns:
        None
    """
    reports = []
    for row in rows_to_insert:
        if not row["is_healthy"]:
            api_name = row["api_url"].split("://")[1].split("/api")[0]

            reports.append(
                f"- ❌{api_name.upper()} está fora do ar!"
            )
    if len(reports) > 0:
        content = "\n".join(reports)
        monitor.send_message(
            title="🚩Algumas APIs estão fora do ar!",
            message=content,
            username="Verificador de Endpoints"
        )
    else:
        monitor.send_message(
            title="✅Todas as APIs estão disponíveis!",
            message="Continuem o bom trabalho pessoal!",
            username="Verificador de Endpoints"
        )