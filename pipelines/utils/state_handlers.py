# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0613
# flake8: noqa: F401, F403

"""
State handlers for prefect tasks
"""

import asyncio
import json
from datetime import datetime

import prefect
import pytz
from discord import Embed
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipelines.utils.infisical import inject_bd_credentials
from pipelines.utils.logger import log
from pipelines.utils.monitor import get_environment, send_discord_embed


def handle_flow_state_change(flow, old_state, new_state):
    environment = get_environment()

    inject_bd_credentials(environment=environment)

    info = {
        "flow_name": flow.name,
        "flow_id": prefect.context.get("flow_id"),
        "flow_run_id": prefect.context.get("flow_run_id"),
        "flow_parameters": json.dumps(prefect.context.get("parameters", {})),
        "state": type(new_state).__name__,
        "message": new_state.message,
        "occurrence": datetime.now(tz=pytz.timezone("America/Sao_Paulo")).isoformat(),
    }

    if new_state.is_failed() and environment == "prod" and len(flow.get_owners()) > 0:
        message = [
            " ".join([f"<@{owner}>" for owner in flow.get_owners()]),
            f"> Flow Run: [{prefect.context.get('flow_run_name')}](https://pipelines.dados.rio/flow-run/{info['flow_run_id']})",
            f"*Parâmetros:*",
        ]
        for key, value in prefect.context.get("parameters", {}).items():
            message.append(f"- {key}: `{value}`")

        asyncio.run(
            send_discord_embed(
                contents=[
                    Embed(
                        title=info["flow_name"],
                        description="\n".join(message),
                        color=15158332,
                    )
                ],
                monitor_slug="error",
            )
        )

    rows = [info]
    # ------------------------------------------------------------
    # Sending data to BigQuery
    # ------------------------------------------------------------
    project_id = "rj-sms-dev" if environment == "dev" else "rj-sms"
    dataset_id = "brutos_prefect_staging"
    table_id = "flow_state_change"

    client = bigquery.Client(project=project_id)

    # Create Dataset if it does not exist
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)
        log(f"Created dataset {dataset_id}")

    # Create Table if it does not exist
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("flow_name", "STRING"),
            bigquery.SchemaField("flow_id", "STRING"),
            bigquery.SchemaField("flow_run_id", "STRING"),
            bigquery.SchemaField("flow_parameters", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("occurrence", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        log(f"Created table {table_id}")

    # Insert rows
    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        log(f"Encountered errors while inserting rows: {errors}")
    else:
        log(f"Rows inserted successfully")

    return new_state
