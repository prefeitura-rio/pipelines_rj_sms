# -*- coding: utf-8 -*-
import datetime
import math
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import pytz

import pipelines.utils.monitor as monitor
from pipelines.utils.credential_injector import authenticated_task as task



@task
def create_description(endpoints_table, results_table):
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
        records = results_table[results_table["api_url"] == endpoint["url"]]
        records.sort_values(by="moment", inplace=True)

        today = datetime.datetime.now(tz=pytz.timezone("Brazil/East"))
        yesterday = today - datetime.timedelta(days=1)

        # Availability
        records_today = records[records.moment > yesterday]
        availability = records_today["is_healthy"].mean()

        # Last healthy record
        last_healthy_record = records[records["is_healthy"]]["moment"].iloc[-1]
        hours_since_last_healthy = math.floor((today - last_healthy_record).total_seconds() / 3600)
        days_since_last_healthy = math.floor(hours_since_last_healthy / 24.0)
        hours_since_last_healthy = hours_since_last_healthy % 24

        if days_since_last_healthy >= 1:
            last_healthy_record_text = f"{days_since_last_healthy} dias e {hours_since_last_healthy} horas atrás"  # noqa
        else:
            last_healthy_record_text = f"{hours_since_last_healthy} horas atrás"

        metrics.append((endpoint["title"], availability, last_healthy_record_text))

    metrics.sort(key=lambda x: x[1], reverse=False)

    reports = []
    for name, availability, last_healthy_record_text in metrics:
        if availability == 1.0:
            line = f"- ✅ **{name.upper()}**: {availability:.0%} de disponibilidade."
        elif availability >= 0.8:
            line = f"- ⚠️ **{name.upper()}**: {availability:.0%} de disponibilidade (última disponibilidade: {last_healthy_record_text})"  # noqa
        else:
            line = f"- ❌ **{name.upper()}**: {availability:.0%} de disponibilidade (última disponibilidade: {last_healthy_record_text})"  # noqa
        reports.append(line)

    report = "\n".join(reports)
    return report


@task
def send_report(description):
    """
    Sends a report with the given plot and description.

    Args:
        plot: The plot to include in the report.
        description: The description of the report.

    Returns:
        None
    """
    monitor.send_message(
        title="Disponibilidade de API nas últimas 24h",
        message=description
    )
    return
