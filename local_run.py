# -*- coding: utf-8 -*-
import argparse

from pipelines.dump_ftp_cnes.flows import sms_dump_cnes
from pipelines.prontuarios.raw.smsrio.flows import sms_prontuarios_raw_smsrio
from pipelines.prontuarios.std.smsrio.flows import smsrio_standardization

# ==================================================
# CONFIGURATION
# --------------------------------------------------
# Please, register here pairs: (flow, param)
# ==================================================
flows_run_cases = [
    (

        smsrio_standardization,
        {
            "start_datetime": "2024-02-06 12:00:00",
            "end_datetime": "2024-02-06 12:00:30"
        }
    ),
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

    print(f"[!] Running flow {flow.name} using as params: {params}.")

    flow.run(**params)
