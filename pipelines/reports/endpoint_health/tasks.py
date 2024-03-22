# -*- coding: utf-8 -*-
import datetime

import numpy as np
import pandas as pd

import pipelines.utils.monitor as monitor
from pipelines.utils.credential_injector import authenticated_task as task


@task
def create_description(endpoints_table, results_table):
    metrics = []
    for _, endpoint in endpoints_table.iterrows():
        records = results_table[results_table["api_url"] == endpoint["url"]]
        records.sort_values(by="moment", inplace=True)

        last_healthy_record = records[records["is_healthy"]].tail(1)["moment"].values[0]  # noqa
        last_healthy_record = last_healthy_record.item().strftime("%d/%m/%Y %H:%M:%S")

        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        records_today = records[
            pd.to_datetime(records.moment).dt.tz_localize(None) > np.datetime64(yesterday)
        ]

        availability = records_today["is_healthy"].mean()

        metrics.append((endpoint["title"], availability, last_healthy_record))

    metrics.sort(key=lambda x: x[1], reverse=False)

    reports = []
    for name, availability, last_healthy in metrics:
        if availability == 1.0:
            line = f"- ✅**{name.upper()}**: {availability:.0%} de disponibilidade."
        elif availability >= 0.9:
            line = f"- ⚠️**{name.upper()}**: {availability:.0%} de disponibilidade.\n   - Última vez disponível em: {last_healthy}"  # noqa
        else:
            line = f"- ❌**{name.upper()}**: {availability:.0%} de disponibilidade.\n   - Última vez disponível em: {last_healthy}"  # noqa
        reports.append(line)

    report = "\n".join(reports)
    return report


@task
def create_plot(endpoints_table, results_table):
    return None


@task
def send_report(plot, description):

    monitor.send_message(title="Disponibilidade de API nas últimas 24h", message=description)
    return
