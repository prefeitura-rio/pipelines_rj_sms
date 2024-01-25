from datetime import date, timedelta

from prefect import task
from pipelines.prontuarios.raw.vitai.utils import (
    group_data_by_cpf
)
from pipelines.utils.tasks import (
    load_from_api
)

from pipelines.prontuarios.raw.vitai.constants import constants as vitai_constants
from pipelines.utils.tasks import (
    get_secret_key
)
from pipelines.prontuarios.raw.vitai.utils import (
    format_date_to_request
)


@task
def get_vitai_api_token(environment: str = 'dev'):
    token = get_secret_key.run(
        secret_path=vitai_constants.INFISICAL_PATH.value,
        secret_name=vitai_constants.INFISICAL_KEY.value,
        environment=environment
    )
    return token


@task
def extract_data_from_api(
    target_day: date,
    entity_name: str,
    vitai_api_token: str
):
    assert entity_name in ['pacientes', 'diagnostico'], f"Invalid entity name: {entity_name}"

    request_url = vitai_constants.API_URL.value + f"{entity_name}/listByPeriodo"

    requested_data = load_from_api.run(
        url=request_url,
        params={
            'dataInicial': format_date_to_request(target_day),
            'dataFinal': format_date_to_request(target_day + timedelta(days=1)),
        },
        credentials=vitai_api_token
    )
    return requested_data


@task
def load_cids_data(
    target_day: date,
    vitai_api_token: str
):
    request_url = vitai_constants.API_URL.value + "diagnostico/listByPeriodo"

    cids_data = load_from_api.run(
        url=request_url,
        params={
            'dataInicial': format_date_to_request(target_day),
            'dataFinal': format_date_to_request(target_day + timedelta(days=1)),
        },
        credentials=vitai_api_token
    )
    return cids_data


@task
def group_patients_data_by_patient(patients_data: list):
    return group_data_by_cpf(
        patients_data,
        lambda data: data['cpf']
    )


@task
def group_cids_data_by_patient(cids_data: list):
    return group_data_by_cpf(
        cids_data,
        lambda data: data['boletim']['paciente']['cpf']
    )
