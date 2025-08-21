# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
import requests
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message


@task
def get_recent_bigquery_jobs(
    environment: str, cost_threshold: float = 0.5, time_threshold: int = 24
):
    query = f"""
    SELECT
        user.email,
        creation_time,
        user.name,
        job_id,
        destination.project_id,
        destination.dataset_id,
        destination.table_id,
        billing_estimated_charge_in_usd as custo_dolar_estimado
    FROM `rj-sms.dashboard_infraestrutura.queries`
    WHERE
        creation_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_threshold} HOUR) and
        billing_estimated_charge_in_usd > {cost_threshold} and
        user.email not like 'prefect%'
    ORDER BY custo_dolar_estimado desc
    """
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result().to_dataframe()

    ontem = (datetime.now() - timedelta(days=1)).strftime("%m-%d-%Y")
    response = requests.get(
        url=f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao='{ontem}'&$format=json"  # noqa: E501
    )
    response.raise_for_status()
    usd_to_brl_rate = response.json()["value"][0]["cotacaoCompra"]

    results["custo_real_estimado"] = results["custo_dolar_estimado"] * usd_to_brl_rate

    return results


@task
def send_discord_alert(environment: str, results: pd.DataFrame):
    lines = []
    for i, row in results.iterrows():
        time = row.creation_time.strftime("%H:%M:%S")
        custo = f"R$ {row.custo_real_estimado:.2f}"
        link = f"https://console.cloud.google.com/bigquery?project={row.project_id}&j=bq:US:{row.job_id}&page=queryresults"
        user = row.email.split("@")[0] + "@..."
        if not row.dataset_id:
            if len(row.table_id) > 20:
                destination = f"`{row.table_id[:20]}...`"
            else:
                destination = f"`{row.table_id}`"
        else:
            destination = f"`{row.dataset_id}`.`{row.table_id}`"

        if row.custo_real_estimado > 5:
            emoji = "⚠️"
        else:
            emoji = ""

        line = f"""- [{time}] {emoji} **{custo}** por `{user}` para {destination}. [Ver detalhes do job]({link})"""  # noqa: E501
        lines.append(line)

    send_message(
        title=f"Alerta de Jobs Caros no BigQuery nas últimas 24h",
        message="\n".join(lines),
        monitor_slug="custo_jobs",
        suppress_embeds=True,
    )
    pass
