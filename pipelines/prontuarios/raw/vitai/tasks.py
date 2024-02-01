"""
Tasks for Vitai Raw Data Extraction
"""
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
def get_vitai_api_token(environment: str = 'dev') -> str:
    """
    Retrieves the API token for the Vitai API.

    Args:
        environment (str, optional): The environment to retrieve the token for. Defaults to 'dev'.

    Returns:
        str: The API token.
    """
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
) -> dict:
    """
    Extracts data from the Vitai API for a specific target day and entity name.

    Args:
        target_day (date): The target day for which data needs to be extracted.
        entity_name (str): The name of the entity for which data needs to be extracted.
                           Valid values are 'pacientes' and 'diagnostico'.
        vitai_api_token (str): The API token for accessing the Vitai API.

    Returns:
        dict: The extracted data from the Vitai API.
    """
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
def group_patients_data_by_patient(patients_data: list) -> dict:
    """
    Groups the patients' data by their CPF.

    Args:
        patients_data (list): A list of patient data dictionaries.

    Returns:
        dict: A dictionary where the keys are the CPFs and the values are lists of patient
              data dictionaries.

    """
    return group_data_by_cpf(
        patients_data,
        lambda data: data['cpf']
    )


@task
def group_cids_data_by_patient(cids_data: list) -> dict:
    """
    Groups the given CID data by patient based on their CPF.

    Args:
        cids_data (list): A list of CID data.

    Returns:
        dict: A dictionary where the keys are CPFs and the values are lists of CID data associated
              with each CPF.
    """
    return group_data_by_cpf(
        cids_data,
        lambda data: data['boletim']['paciente']['cpf']
    )
