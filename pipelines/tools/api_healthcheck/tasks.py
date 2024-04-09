# -*- coding: utf-8 -*-
import datetime
import pytz
import prefect
import requests
import json

from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_api_url(api_url_table, tag):
    if tag == "":
        return api_url_table.to_dict("records")
    api_url_table = api_url_table[api_url_table['tag'] == tag]
    return api_url_table.to_dict("records")


@task
def check_api_health(api_info: dict):
    logger = prefect.context.get("logger")

    endpoint_url = f"{api_info['url']}{api_info['target_endpoint']}"
    logger.info(f"Checking API health for {endpoint_url}")

    try:
        response = requests.get(
            url=endpoint_url,
            timeout=90
        )
    except requests.Timeout:
        logger.error(f"API {api_info['url']} -> Timeout")
        duration = 90
        status_code = "408"
        is_healthy = False
        detail = {
            "status_code": status_code,
            "message": "Timeout",
        }
    else:
        duration = response.elapsed.total_seconds()
        status_code = str(response.status_code)
        is_active = status_code == str(api_info["expected_status_code"])
        is_up = True

        try:
            content = response.json()
            if "status" in content:
                is_up = content["status"] == "UP"
        except requests.exceptions.JSONDecodeError:
            content = response.text

        is_healthy = is_active and is_up

        detail = {
            "status_code": status_code,
            "message": response.reason,
            "content": content
        }
    logger.info(f"API Health {is_healthy}; Duration {duration}s; Status {status_code}")
    
    return {
        "endpoint_id": api_info["id"],
        "is_healthy": is_healthy,
        "duration": duration,
        "moment": datetime.datetime.now(tz=pytz.timezone("Brazil/East")).isoformat(),
        "detail": json.dumps(detail)
    }


@task
def insert_results(rows_to_insert: list[dict]):
    logger = prefect.context.get("logger")

    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")
    table = bq_client.get_table(
        "rj-sms.gerenciamento__monitoramento_de_api.endpoint_ping"
    )

    errors = bq_client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        logger.info(f"Inserted {len(rows_to_insert)} rows")
    else:
        logger.error(f"Errors: {errors}")
        raise ValueError(f"Errors: {errors}")
