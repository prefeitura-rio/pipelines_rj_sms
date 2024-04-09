# -*- coding: utf-8 -*-
import datetime
import math

import pandas as pd
import pytz

import pipelines.utils.monitor as monitor
from pipelines.utils.credential_injector import authenticated_task as task


@task
def filter_dataframe_using_tag(dataframe, tag):
    """
    Filter a DataFrame using a tag.

    Args:
        dataframe (pandas.DataFrame): DataFrame to be filtered.
        tag (str): Tag to be used to filter the DataFrame.

    Returns:
        pandas.DataFrame: Filtered DataFrame.

    """
    if tag == "":
        return dataframe
    
    return dataframe[dataframe["tag"] == tag]

@task
def create_and_send_report(endpoints_table, results_table, filter_tag):
    """
    Creates a description of endpoint health metrics.

    Args:
        endpoints_table (pandas.DataFrame): DataFrame containing information about the endpoints.
        results_table (pandas.DataFrame): DataFrame containing the results of health checks.

    Returns:
        str: A formatted report.

    """
    metrics = []
    results_table["moment"] = pd.to_datetime(results_table["moment"]).dt.tz_convert("Brazil/East")

    # Iterate over each endpoint
    for _, endpoint in endpoints_table.iterrows():
        records = results_table[results_table["endpoint_id"] == endpoint["id"]]
        records.sort_values(by="moment", inplace=True)

        today = datetime.datetime.now(tz=pytz.timezone("Brazil/East"))
        yesterday = today - datetime.timedelta(days=1)

        # Availability
        records_today = records[records.moment > yesterday]
        availability = records_today["is_healthy"].mean()

        # Last healthy record
        healthy_records = records[records["is_healthy"]]
        if not healthy_records.empty:
            last_healthy_record = healthy_records["moment"].iloc[-1]
            hours_since_last_healthy = math.floor(
                (today - last_healthy_record).total_seconds() / 3600
            )  # noqa
            days_since_last_healthy = math.floor(hours_since_last_healthy / 24.0)
            hours_since_last_healthy = hours_since_last_healthy % 24

            if days_since_last_healthy >= 1:
                last_healthy_record_text = (
                    f"{days_since_last_healthy}d e {hours_since_last_healthy}hs atrás"  # noqa
                )
            else:
                last_healthy_record_text = f"{hours_since_last_healthy}hs atrás"
        else:
            last_healthy_record_text = "nunca"

        metrics.append((endpoint["slug"], availability, last_healthy_record_text))

    metrics.sort(key=lambda x: x[1], reverse=False)

    reports = []
    for name, availability, last_healthy_record_text in metrics:
        if availability == 1.0:
            line = f"- ✅ **{name.upper()}**: {availability:.0%} de disp."
        elif availability >= 0.8:
            line = f"- ⚠️ **{name.upper()}**: {availability:.0%} de disp. (última disp.: {last_healthy_record_text})"  # noqa
        else:
            line = f"- ❌ **{name.upper()}**: {availability:.0%} de disp. (última disp.: {last_healthy_record_text})"  # noqa
        reports.append(line)

    if filter_tag != "":
        body = f"### Método: `{filter_tag}`\n" + "\n".join(reports)
    else:
        body = "\n".join(reports)

    monitor.send_message(
        title="Disponibilidade de API nas últimas 24h",
        message=body,
        monitor_slug="endpoint-health",
    )
