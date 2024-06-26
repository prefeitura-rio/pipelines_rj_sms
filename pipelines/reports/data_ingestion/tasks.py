# -*- coding: utf-8 -*-
from datetime import date, timedelta

import pandas as pd

from pipelines.reports.data_ingestion.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_secret_key


@task
def get_prontuarios_database_url(environment):
    """
    Get Prontuarios database url from Infisical Secrets.

    Args:
        environment (str): Environment

    Returns:
        str: Database url
    """
    database_url = get_secret_key.run(
        secret_path="/",
        secret_name=constants.INFISICAL_PRONTUARIOS_DB_URL.value,
        environment=environment,
    )
    return database_url


@task
def get_target_date(custom_target_date: str) -> date:
    """
    Returns the target date based on the provided custom_target_date.

    Args:
        custom_target_date (str): The custom target date in ISO format (YYYY-MM-DD).

    Returns:
        date: The target date.

    Raises:
        ValueError: If the custom_target_date is not in the correct format.

    """
    if custom_target_date == "":
        return date.today() - timedelta(days=1)
    else:
        return date.fromisoformat(custom_target_date)


@task(retry_delay=timedelta(minutes=5), max_retries=3)
def get_records_summary(
    db_url: str,
    target_date: date,
) -> pd.DataFrame:

    records = pd.read_sql(
        f"""
        select distinct on (raw.id, std.id, mrg.patient_code)
            datasource.system as datasource,
            raw.patient_code,
            raw.id as raw_id, raw.created_at as raw_created_at,
            std.id as std_id, std.created_at as std_created_at,
            mrg.patient_code as mrg_id, mrg.updated_at as mrg_updated_at
        from raw__patientrecord raw
            inner join datasource on raw.data_source_id = datasource.cnes
            left join std__patientrecord std on raw.id = std.raw_source_id
            left join mrg__patient mrg
                on mrg.patient_code = std.patient_code and mrg.updated_at > std.created_at
        where date(raw.created_at) = '{target_date}';
        """,
        db_url,
    )
    return records


@task
def create_report(
    target_date: date,
    records_summary: pd.DataFrame,
) -> None:
    """
    Creates a raw report from the raw data.

    Args:
        data (pd.DataFrame): The dataframe.

    Returns:
        None
    """
    formatted_date = target_date.strftime("%d/%m/%Y")

    raw = {
        "vitai_total": records_summary[records_summary["datasource"] == "vitai"][
            "raw_id"
        ].nunique(),  # noqa
        "vitai_unique": records_summary[records_summary["datasource"] == "vitai"][
            "patient_code"
        ].nunique(),  # noqa
        "vitacare_total": records_summary[records_summary["datasource"] == "vitacare"][
            "raw_id"
        ].nunique(),  # noqa
        "vitacare_unique": records_summary[records_summary["datasource"] == "vitacare"][
            "patient_code"
        ].nunique(),  # noqa
        "smsrio_total": records_summary[records_summary["datasource"] == "smsrio"][
            "raw_id"
        ].nunique(),  # noqa
        "smsrio_unique": records_summary[records_summary["datasource"] == "smsrio"][
            "patient_code"
        ].nunique(),  # noqa
        "total": records_summary["raw_id"].nunique(),
        "unique": records_summary["patient_code"].nunique(),
    }
    std = {
        "vitai_total": records_summary[records_summary["datasource"] == "vitai"][
            "std_id"
        ].nunique(),  # noqa
        "vitai_unique": records_summary[records_summary["datasource"] == "vitai"][
            "patient_code"
        ].nunique(),  # noqa
        "vitacare_total": records_summary[records_summary["datasource"] == "vitacare"][
            "std_id"
        ].nunique(),  # noqa
        "vitacare_unique": records_summary[records_summary["datasource"] == "vitacare"][
            "patient_code"
        ].nunique(),  # noqa
        "smsrio_total": records_summary[records_summary["datasource"] == "smsrio"][
            "std_id"
        ].nunique(),  # noqa
        "smsrio_unique": records_summary[records_summary["datasource"] == "smsrio"][
            "patient_code"
        ].nunique(),  # noqa
        "total": records_summary["std_id"].nunique(),
        "unique": records_summary["patient_code"].nunique(),
    }
    mrg = {
        "vitai": records_summary[records_summary["datasource"] == "vitai"]["mrg_id"].nunique(),
        "vitacare": records_summary[records_summary["datasource"] == "vitacare"][
            "mrg_id"
        ].nunique(),  # noqa
        "smsrio": records_summary[records_summary["datasource"] == "smsrio"]["mrg_id"].nunique(),
        "total": records_summary["mrg_id"].nunique(),
    }

    def percent(x, y):
        try:
            return f"{(x/y)*100:.2f}%"
        except ZeroDivisionError:
            return "0%"

    df = pd.DataFrame(
        [
            [
                f"{raw['vitacare_unique']} ({raw['vitacare_total']})",
                f"{std['vitacare_unique']} ({std['vitacare_total']})",
                f"{mrg['vitacare']}",
                f"{percent(mrg['vitacare'], std['vitacare_unique'])}",
            ],
            [
                f"{raw['vitai_unique']} ({raw['vitai_total']})",
                f"{std['vitai_unique']} ({std['vitai_total']})",
                f"{mrg['vitai']}",
                f"{percent(mrg['vitai'], std['vitai_unique'])}",
            ],
            [
                f"{raw['smsrio_unique']} ({raw['smsrio_total']})",
                f"{std['smsrio_unique']} ({std['smsrio_total']})",
                f"{mrg['smsrio']}",
                f"{percent(mrg['smsrio'], std['smsrio_unique'])}",
            ],
            ["", "", "", ""],
            [
                f"{raw['unique']} ({raw['total']})",
                f"{std['unique']} ({std['total']})",
                f"{mrg['total']}",
                f"{percent(mrg['total'], std['unique'])}",
            ],
        ],
        columns=["RAW", "STD", "MRG", "%"],
        index=["VITACARE", "VITAI", "SMSRIO", "", "Total"],
    )

    send_message(
        title=f"Ingestão Diária de Dados Brutos: {formatted_date}",
        message=f"""
## Pacientes
```
{df.to_markdown()}
```
""",
        monitor_slug="data-ingestion",
    )
    return None
