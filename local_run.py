# -*- coding: utf-8 -*-
import argparse

#from pipelines.prontuarios.raw.smsrio.flows import sms_prontuarios_raw_smsrio
#from pipelines.prontuarios.std.vitai.flows import vitai_standardization
#from pipelines.prontuarios.std.smsrio.flows import smsrio_standardization
#from pipelines.prontuarios.std.extracao.smsrio.flows import smsrio_standardization_historical
from pipelines.prontuarios.std.extracao.extracao_inicial import smsrio_standardization_historical_all

# ==================================================
# CONFIGURATION
# --------------------------------------------------
# Please, register here pairs: (flow, param)
# ==================================================
flows_run_cases = [
    # (
    #     vitai_standardization,
    #     {"start_datetime": "2024-02-26 05:44:01", "end_datetime": "2024-02-26 05:44:10"},
    # ),
    (
        smsrio_standardization_historical_all,
        {"source_start_datetime": "2024-03-10", "source_end_datetime": "2024-03-11"},
    )
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
