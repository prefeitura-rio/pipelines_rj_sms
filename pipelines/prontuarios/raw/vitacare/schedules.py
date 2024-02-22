# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitacare Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks
from pipelines.utils.tasks import load_file_from_bigquery
from pipelines.utils.infisical import inject_bd_credentials


#####################################
# Parameters
#####################################

# Inject the credentials for Google Services
inject_bd_credentials(environment="prod")

# Access the health units information from BigQuery table
dados_mestres = load_file_from_bigquery.run(
    project_name="rj-sms",
    dataset_name="saude_dados_mestres",
    table_name="estabelecimento"
)
# Filter the units using Vitacare
unidades_vitacare = dados_mestres[dados_mestres["prontuario_versao"] == 'vitacare']

# Get their CNES list
cnes_vitacare = unidades_vitacare["id_cnes"].tolist()

# Construct the parameters for the flow
vitacare_flow_parameters = []
for entity in ["diagnostico"]:
    for cnes in cnes_vitacare:
        vitacare_flow_parameters.append(
            {
                "cnes": cnes,
                "entity": entity,
                "environment": "prod",
                "rename_flow": True,
            }
        )

#####################################
# Schedules
#####################################

vitacare_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=vitacare_flow_parameters,
    runs_interval_minutes=0,
)

vitacare_daily_update_schedule = Schedule(clocks=untuple_clocks(vitacare_clocks))
