# -*- coding: utf-8 -*-
import argparse

from pipelines.execute_dbt.flows import sms_execute_dbt
from pipelines.reports.endpoint_health.flows import disponibilidade_api
from pipelines.tools.api_healthcheck.flows import monitoramento_api

# ==================================================
# CONFIGURATION
# --------------------------------------------------
# Please, register here pairs: (flow, param)
# ==================================================
flows_run_cases = [
    (
        sms_execute_dbt,
        {
            "command": "test",
            "environment": "dev",
            "model": None,
        },
    ),
    (disponibilidade_api, {}),
    (monitoramento_api, {}),
]


# ==================================================
# CODE
# ==================================================

parser = argparse.ArgumentParser(description="Run a specific flow")

parser.add_argument("--case", type=int, help="The index of the pair (flow, param) to run")

parser.add_argument(
    "--environment", type=str, help="The environment to run the flow on", default="dev"
)

if __name__ == "__main__":
    args = parser.parse_args()

    flow, params = flows_run_cases[args.case]

    params["environment"] = args.environment
    params["run_on_schedule"] = False

    print(f"[!] Running flow {flow.name} using as params: {params}.")
    flow.run(**params)
