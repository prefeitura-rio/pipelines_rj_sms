# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
import requests

from pipelines.datalake.extract_load.vitai_api.tasks import get_all_api_data
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key


@task(max_retries=3, retry_delay=timedelta(seconds=10))
def vitai_db_health_check(enviroment: str):

    db_url = get_secret_key.run(
        environment=enviroment,
        secret_name="DB_URL",
        secret_path="/prontuario-vitai",
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
def vitai_api_health_check(enviroment: str):

    all_api = get_all_api_data.run(
        environment=enviroment,
    )

    api_token = get_secret_key.run(
        environment=enviroment,
        secret_name="TOKEN",
        secret_path="/prontuario-vitai",
    )
    results = []
    for api in all_api:
        start_time = datetime.now()
        try:
            response = requests.get(
                url=f"{api['api_url']}/v1/atendimento/listByPeriodo",
                headers={"Authorization": f"Bearer {api_token}"},
                params={"dataInicial": "23/07/2025 09:00:00", "dataFinal": "23/07/2025 12:00:00"},
                timeout=90,
            )
            response.raise_for_status()
        except Exception as e:
            final_time = datetime.now()
            log(f"Vitai API Health Check failed: {e}")
            results.append(
                {
                    "is_healthy": False,
                    "slug": "vitai_api",
                    "message": f"Vitai API Health Check failed: {e}",
                    "duration": (final_time - start_time).total_seconds(),
                    "created_at": datetime.now(),
                }
            )
        else:
            final_time = datetime.now()
            log(f"Vitai API Health Check succeeded: {api['cnes']}")
            results.append(
                {
                    "is_healthy": True,
                    "slug": f"vitai_api_{api['cnes']}",
                    "message": f"Vitai API Health Check succeeded: {api['cnes']}",
                    "duration": (final_time - start_time).total_seconds(),
                    "created_at": datetime.now(),
                }
            )
    return results


@task
def print_result(results: list, enviroment: str):
    results_as_str = "\n".join(results)
    log(f"Health Check Results: {results_as_str}")
    return


@task
def transform_to_df(results_smsrio: list, results_vitai_db: list, results_vitai_api: list):
    results = []
    results.extend(results_smsrio)
    results.extend(results_vitai_db)
    results.extend(results_vitai_api)
    return pd.DataFrame(results)
