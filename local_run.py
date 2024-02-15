import argparse

from pipelines.prontuarios.raw.vitai.flows import vitai_extraction
from pipelines.prontuarios.raw.smsrio.flows import sms_prontuarios_raw_smsrio

# ==================================================
# CONFIGURATION
# --------------------------------------------------
# Please, register here pairs: (flow, param)
# ==================================================
flows_run_cases = [
    (vitai_extraction, {'cnes': '5717256', 'entity': 'diagnostico', 'minimum_date': '2024-02-01', "api_url": "http://api.token.hmrg.vitai.care/api/v1/"}),
    (vitai_extraction, {'cnes': '5717256', 'entity': 'pacientes', 'minimum_date': '', "api_url": "http://api.token.hmrg.vitai.care/api/v1/"}),
    (sms_prontuarios_raw_smsrio, {'is_inicial_extraction': False})
]


# ==================================================
# CODE
# ==================================================

parser = argparse.ArgumentParser(
    description='Run a specific flow'
)

parser.add_argument(
    '--case', 
    type=int, 
    help='The index of the pair (flow, param) to run'
)

parser.add_argument(
    '--environment', 
    type=str, 
    help='The environment to run the flow on', 
    default='dev'
)

if __name__ == '__main__':
    args = parser.parse_args()
    
    flow, params = flows_run_cases[args.case]

    params['environment'] = args.environment
    params['run_on_schedule'] = False
    params['rename_flow'] = False

    print(f"[!] Running flow {flow.name} using as params: {params}.")

    flow.run(**params)
