# -*- coding: utf-8 -*-
import json
from datetime import datetime, timedelta

import pandas as pd
import requests

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key, load_file_from_bigquery


@task
def get_all_api_data(environment: str = "dev") -> str:

    df = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name="saude_dados_mestres",
        table_name="estabelecimento",
    )
    df = df[(df["prontuario_versao"] == "vitai") & (df["prontuario_episodio_tem_dado"] == "sim")]

    cnes_list = df["id_cnes"].tolist()

    api_data = []
    for cnes in cnes_list:
        try:
            api_url = get_secret_key.run(
                environment=environment,
                secret_name=f"API_URL_{cnes}",
                secret_path="/prontuario-vitai",
            )
        except Exception:
            log(f"A URL da API não foi encontrada para o CNES {cnes}")
            continue

        api_data.append(
            {
                "cnes": cnes,
                "api_url": api_url,
            }
        )

    return api_data


@task
def extract_data(
    api_data: str, api_token: str, endpoint_path: str, target_date: str
) -> pd.DataFrame:

    endpoint_url = f"{api_data['api_url']}{endpoint_path}"

    if "listByPeriodo" in endpoint_path:
        # Dividir data em lista de 24 janelas de 1h
        windows = []
        for i in range(24):
            windows.append(
                (
                    (target_date + timedelta(hours=i)).strftime("%d/%m/%Y %H:00:00"),
                    (target_date + timedelta(hours=i + 1)).strftime("%d/%m/%Y %H:00:00"),
                )
            )

        responses = []
        for window in windows:
            endpoint_params = {
                "dataInicial": window[0],
                "dataFinal": window[1],
            }
            try:
                response = requests.get(
                    endpoint_url,
                    headers={"Authorization": f"Bearer {api_token}"},
                    params=endpoint_params,
                    timeout=90,
                )
                response.raise_for_status()
                responses.extend(response.json())
            except Exception as e:
                log(f"Error extracting data from {endpoint_url}: {e}")
                continue

        return responses
    elif "{estabelecimento_id}" in endpoint_path:
        response = requests.get(
            f"{api_data['api_url']}/v1/estabelecimentos/",
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=90,
        )
        response.raise_for_status()

        estabelecimento_id_list = [x["id"] for x in response.json()]

        responses = []
        for estabelecimento_id in estabelecimento_id_list:
            response = requests.get(
                endpoint_url.format(estabelecimento_id=estabelecimento_id),
                headers={"Authorization": f"Bearer {api_token}"},
                timeout=90,
            )
            response.raise_for_status()
            responses.extend(response.json())

        return responses
    else:
        response = requests.get(
            endpoint_url,
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=90,
        )
        response.raise_for_status()
        return response.json()


@task
def format_to_upload(responses: list, endpoint_path: str, target_date: str) -> pd.DataFrame:

    flattened_responses = []
    for response in responses:
        flattened_responses.extend(response)

    rows = [
        {
            "data": json.dumps(row),
            "_loaded_at": datetime.now(),
            "_endpoint": endpoint_path,
            "_target_date": target_date,
        }
        for row in flattened_responses
    ]
    df = pd.DataFrame(rows)
    return df
