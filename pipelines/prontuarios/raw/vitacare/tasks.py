# -*- coding: utf-8 -*-
"""
Tasks for Vitacare Raw Data Extraction
"""
import json
from datetime import date, timedelta

import numpy as np
import prefect
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed

from pipelines.prontuarios.raw.vitacare.constants import constants as vitacare_constants
from pipelines.prontuarios.utils.misc import build_additional_fields, split_dataframe

# from prefect import task
from pipelines.utils.credential_injector import gcp_task as task
from pipelines.utils.stored_variable import stored_variable_converter
from pipelines.utils.tasks import (
    cloud_function_request,
    get_secret_key,
    load_file_from_bigquery,
    load_file_from_gcs_bucket,
)


@task(max_retries=4, retry_delay=timedelta(minutes=3))
@stored_variable_converter(output_mode="original")
def extract_data_from_api(
    cnes: str, ap: str, target_day: date, entity_name: str, environment: str = "dev"
) -> dict:
    api_url = vitacare_constants.AP_TO_API_URL.value[ap]
    endpoint = vitacare_constants.ENTITY_TO_ENDPOINT.value[entity_name]

    logger = prefect.context.get("logger")
    logger.info(f"Extracting data for {entity_name} on {target_day}")

    username = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    response = cloud_function_request.run(
        url=f"{api_url}{endpoint}",
        request_type="GET",
        query_params={"date": str(target_day), "cnes": cnes},
        credential={"username": username, "password": password},
        env=environment,
    )

    if response["status_code"] != 200:
        raise ValueError(f"Failed to extract data from API: {response['status_code']}")

    requested_data = json.loads(response["body"])

    if len(requested_data) > 0:
        logger.info(f"Successful Request: retrieved {len(requested_data)} registers")
    else:
        logger.error("Failed Request: no data was retrieved")
        raise ENDRUN(state=Failed(f"Empty response for ({cnes}, {target_day}, {entity_name})"))

    return requested_data


@task(max_retries=3, retry_delay=timedelta(minutes=3))
@stored_variable_converter(output_mode="original")
def extract_data_from_dump(cnes: str, ap: str, entity_name: str, environment: str = "dev") -> dict:
    """
    Extracts data from a dump file in a GCS bucket.

    Args:
        cnes (str): The CNES value.
        ap (str): The AP value.
        entity_name (str): The name of the entity.
        environment (str, optional): The environment to use. Defaults to "dev".

    Returns:
        dict: A dictionary containing the extracted data.

    """
    assert (
        entity_name in vitacare_constants.ENTITY_TO_ENDPOINT.value
    ), f"Invalid entity name: {entity_name}"

    dataframe = load_file_from_gcs_bucket.run(
        bucket_name=vitacare_constants.BUCKET_NAME.value,
        file_name=vitacare_constants.DUMP_PATH_TEMPLATE.value.format(ENTITY=entity_name, AP=ap[2:]),
        file_type="csv",
        csv_sep=";" if entity_name == "pacientes" else ",",
    )
    # Filtro por CNES
    cnes_column = "NUMERO_CNES_UNIDADE" if entity_name == "pacientes" else "NUMERO_CNES_DA_UNIDADE"
    dataframe = dataframe[dataframe[cnes_column] == cnes]

    # Standardize column names
    dataframe.rename(
        columns={
            # Transformação do Dump de Diagnostico
            "N_CPF": "cpfPaciente",
            "DATA_NASC_PACIENTE": "dataNascPaciente",
            "DATA_CONSULTA": "dataConsulta",
            # Transformação do Dump de Paciente
            "CPF_PACIENTE": "cpfPaciente",
            "DATA_DE_NASCIMENTO": "dataNascPaciente",
            "DATA_ULTIMA_ATUALIZACAO_DO_CADASTRO": "dataConsulta",
        },
        inplace=True,
    )

    # Replace NaN with None
    dataframe.replace(np.nan, None, inplace=True)

    # Slice DF into multiple chunks
    dataframes = split_dataframe(dataframe, chunk_size=1000)

    # Transform Chunks to Json
    records = [df.to_dict(orient="records") for df in dataframes]

    return records


@task
@stored_variable_converter()
def group_data_by_patient(data: list[dict], entity_type: str) -> dict:
    """
    Groups the data list by patient CPF.

    Args:
        data (list): The list of dict data.
        entity (str): The entity name. This is used to choose the path to the
            patient cpf.

    Returns:
        dict: A dictionary where the keys are patient identifiers and the values are
            lists of entity data.

    Raises:
        ValueError: If the entity name is invalid.
    """
    if entity_type == "diagnostico":
        return build_additional_fields(
            data_list=data,
            cpf_get_function=lambda data: data["cpfPaciente"],
            birth_data_get_function=lambda data: data["dataNascPaciente"],
            source_updated_at_get_function=lambda data: data["dataConsulta"],
        )
    elif entity_type == "pacientes":
        return build_additional_fields(
            data_list=data,
            cpf_get_function=lambda data: data["cpfPaciente"],
            birth_data_get_function=lambda data: data["dataNascPaciente"],
            source_updated_at_get_function=lambda data: data["dataConsulta"],
        )
    else:
        raise ValueError(f"Invalid entity type: {entity_type}")


@task
def create_parameter_list(environment: str = "dev", initial_extraction: bool = False):
    """
    Create a list of parameters for running the Vitacare flow.

    Args:
        environment (str, optional): The environment to run the flow in. Defaults to "dev".

    Returns:
        list: A list of dictionaries containing the flow run parameters.
    """
    # Access the health units information from BigQuery table
    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name="saude_dados_mestres",
        table_name="estabelecimento",
    )
    # Filter the units using Vitacare
    unidades_vitacare = dados_mestres[dados_mestres["prontuario_versao"] == "vitacare"]

    # Get their CNES list
    cnes_vitacare = unidades_vitacare["id_cnes"].tolist()

    # Get the date of yesterday
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Construct the parameters for the flow
    vitacare_flow_parameters = []
    for entity in ["diagnostico"]:
        for cnes in cnes_vitacare:
            vitacare_flow_parameters.append(
                {
                    "cnes": cnes,
                    "entity": entity,
                    "target_date": yesterday,
                    "initial_extraction": initial_extraction,
                    "environment": environment,
                    "rename_flow": True,
                }
            )

    logger = prefect.context.get("logger")
    logger.info(f"Created {len(vitacare_flow_parameters)} flow run parameters")

    return vitacare_flow_parameters
