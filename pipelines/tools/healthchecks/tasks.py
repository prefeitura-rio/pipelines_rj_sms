# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd

from pipelines.datalake.extract_load.vitacare_api.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import cloud_function_request, get_secret_key


@task
def get_ap_list():
    log("Getting AP list...")
    ap_list = list(vitacare_constants.BASE_URL.value.keys())
    log(f"AP list: {ap_list}")
    return ap_list


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def vitai_db_health_check(enviroment: str):

    db_url = get_secret_key.run(
        environment=enviroment, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    try:
        start_time = datetime.now()
        pd.read_sql("SELECT 1", db_url)
        final_time = datetime.now()
        log("Vitai DB Health Check succeeded.")
    except Exception as e:
        log(f"Vitai DB Health Check failed: {e}")
        return [
            {
                "is_healthy": False,
                "slug": "vitai_db",
                "message": f"Vitai DB Health Check failed: {e}",
                "duration": None,
                "created_at": datetime.now(),
            }
        ]
    else:
        return [
            {
                "is_healthy": True,
                "slug": "vitai_db",
                "message": "Vitai DB Health Check succeeded.",
                "duration": (final_time - start_time).total_seconds(),
                "created_at": datetime.now(),
            }
        ]


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def smsrio_db_health_check(enviroment: str):

    db_url = get_secret_key.run(
        environment=enviroment,
        secret_name="DB_URL",
        secret_path="/smsrio",
    )

    try:
        start_time = datetime.now()
        pd.read_sql("SELECT 1", f"{db_url}/sms_pacientes")
        final_time = datetime.now()
        log("SMSRio DB Health Check succeeded.")
    except Exception as e:
        log(f"SMSRio DB Health Check failed: {e}")
        return [
            {
                "is_healthy": False,
                "slug": "smsrio_db",
                "message": f"SMSRio DB Health Check failed: {e}",
                "duration": None,
                "created_at": datetime.now(),
            }
        ]
    else:
        return [
            {
                "is_healthy": True,
                "slug": "smsrio_db",
                "message": "SMSRio DB Health Check succeeded.",
                "duration": (final_time - start_time).total_seconds(),
                "created_at": datetime.now(),
            }
        ]


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def vitacare_api_health_check(enviroment: str, ap: str):
    log(f"Checking Vitacare API health for AP: {ap}")

    username = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=enviroment,
    )
    password = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=enviroment,
    )

    api_url = vitacare_constants.BASE_URL.value[ap]
    log(f"API URL: {api_url}")

    endpoint_url = vitacare_constants.ENDPOINT.value["posicao"]
    log(f"Endpoint URL: {endpoint_url}")

    try:
        start_time = datetime.now()
        cloud_function_request.run(
            url=f"{api_url}{endpoint_url}",
            request_type="GET",
            query_params={"date": str(datetime.now().date()), "cnes": None},
            credential={"username": username, "password": password},
            env=enviroment,
        )
        final_time = datetime.now()
    except Exception as e:
        log(f"Vitacare API Health Check failed: {e}")
        return [
            {
                "is_healthy": False,
                "slug": f"vitacare_api_{ap.lower()}",
                "message": f"Vitacare API Health Check failed: {e}",
                "duration": None,
                "created_at": datetime.now(),
            }
        ]
    else:
        return [
            {
                "is_healthy": True,
                "slug": f"vitacare_api_{ap.lower()}",
                "message": "Vitacare API Health Check succeeded.",
                "duration": (final_time - start_time).total_seconds(),
                "created_at": datetime.now(),
            }
        ]

    return results


@task
def print_result(results: list, enviroment: str):
    results_as_str = "\n".join(results)
    log(f"Health Check Results: {results_as_str}")
    return


@task
def transform_to_df(results: list):
    return pd.DataFrame(results)
