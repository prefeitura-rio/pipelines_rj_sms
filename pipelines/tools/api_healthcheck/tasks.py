import datetime
from prefect import task
import requests
import prefect
from google.cloud import bigquery


@task
def get_api_url(api_url_list):
    return api_url_list.to_dict('records')

@task
def check_api_health(api_info:dict):
    logger = prefect.context.get("logger")
    logger.info(f"Checking API health for {api_info['url']}")

    try:
        response = requests.get(api_info['url'], timeout=60)
    except TimeoutError:
        logger.error(f"API {api_info['url']} -> Timeout")
        duration = None
        status_code = '408'
        is_healthy = False
    else:
        duration = response.elapsed.total_seconds()
        status_code = str(response.status_code)
        is_healthy = status_code == str(api_info['expected_status_code'])

    logger.info(f"API Health {is_healthy}; Duration {duration}s; Status {status_code}")

    return {
        'api_url': api_info['url'],
        'is_healthy': is_healthy,
        'duration': duration,
        'status_code': status_code,
        'moment': datetime.datetime.now().isoformat()
    }

@task
def insert_results(rows_to_insert:list[dict]):
    logger = prefect.context.get("logger")

    bq_client = bigquery.Client.from_service_account_json('/tmp/credentials.json')
    table = bq_client.get_table("rj-sms.gerenciamento.api_health_check")

    errors = bq_client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        logger.info(f"Inserted {len(rows_to_insert)} rows")
    else:
        logger.error(f"Errors: {errors}")
        raise ValueError(f"Errors: {errors}")