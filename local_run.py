# -*- coding: utf-8 -*-
import argparse

from pipelines.dump_ftp_cnes.flows import sms_dump_cnes
from pipelines.prontuarios.raw.smsrio.flows import sms_prontuarios_raw_smsrio
from pipelines.prontuarios.raw.vitacare.flows import vitacare_extraction
from pipelines.prontuarios.raw.vitai.flows import vitai_extraction

# ==================================================
# CONFIGURATION
# --------------------------------------------------
# Please, register here pairs: (flow, param)
# ==================================================
flows_run_cases = [
    (
        vitacare_extraction,
        {
            "cnes": "5621801",
            "entity": "diagnostico",
            "minimum_date": "2024-02-15",
        },
    ),
    (
        vitai_extraction,
        {
            "cnes": "5717256",
            "entity": "diagnostico",
            "minimum_date": "2023-02-21",
        },
    ),
    (sms_dump_cnes, {}),
    (sms_prontuarios_raw_smsrio, {"is_initial_extraction": False}),
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
    params["rename_flow"] = False

    print(f"[!] Running flow {flow.name} using as params: {params}.")

    flow.run(**params)
