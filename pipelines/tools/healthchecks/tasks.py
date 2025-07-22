# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key


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


@task
def print_result(results: list, enviroment: str):
    results_as_str = "\n".join(results)
    log(f"Health Check Results: {results_as_str}")
    return


@task
def transform_to_df(results_smsrio: list, results_vitai: list):
    results = []
    results.extend(results_smsrio)
    results.extend(results_vitai)
    return pd.DataFrame(results)
