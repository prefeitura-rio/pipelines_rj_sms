# -*- coding: utf-8 -*-
import json
from datetime import datetime

import pandas as pd
import pytz
from pandas import Timestamp

from pipelines.datalake.extract_load.vitacare_api_v2.constants import (
    constants as flow_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import (
    cloud_function_request,
    get_secret_key,
    load_file_from_bigquery,
)


@task(nout=2)
def generate_endpoint_params(
    target_date: Timestamp, environment: str = "dev", table_id_prefix: str = None
) -> tuple[dict, list]:

    # Adquirir credenciais
    username = get_secret_key.run(
        secret_path=flow_constants.INFISICAL_PATH.value,
        secret_name=flow_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=flow_constants.INFISICAL_PATH.value,
        secret_name=flow_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    # Listagem de estabelecimentos por AP
    estabelecimentos = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name="saude_dados_mestres",
        table_name="estabelecimento",
    )[["id_cnes", "area_programatica", "prontuario_versao"]]

    estabelecimentos = estabelecimentos[estabelecimentos["prontuario_versao"] == "vitacare"]
    estabelecimentos = estabelecimentos.groupby("area_programatica").agg(
        cnes_list=("id_cnes", list)
    )

    params = []
    table_names = []
    for ap, df in estabelecimentos.iterrows():
        params.append(
            {
                "ap": f"AP{ap}",
                "cnes_list": df["cnes_list"],
                "target_date": target_date.strftime("%Y-%m-%d"),
                "username": username,
                "password": password,
            }
        )
        table_names.append(f"{table_id_prefix}_ap{ap}")

    return params, table_names


@task
def extract_data(endpoint_params: dict, endpoint_name: str, environment: str = "dev") -> dict:

    log(
        f"Extracting data from API: {endpoint_params['ap']} {endpoint_name}."
        + f" There are {len(endpoint_params['cnes_list'])} CNES to extract."
    )
    api_url = (
        flow_constants.BASE_URL.value[endpoint_params["ap"]]
        + flow_constants.ENDPOINT.value[endpoint_name]
    )
    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    extracted_data = []

    for cnes in endpoint_params["cnes_list"]:
        log(
            f"Extracting data from API: ({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
        )
        try:
            response = cloud_function_request.run(
                url=api_url,
                endpoint_for_filename=endpoint_name,
                request_type="GET",
                query_params={"date": str(endpoint_params["target_date"]), "cnes": cnes},
                credential={
                    "username": endpoint_params["username"],
                    "password": endpoint_params["password"],
                },
                env=environment,
            )
        except Exception as e:
            log(
                "Error extracting data from API:"
                + f" ({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
                + f" {e}"
            )
            continue

        if response["status_code"] != 200:
            log(
                "Error extracting data from API:"
                + f" ({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
                + f" {response['status_code']}"
            )
            continue

        rows = [json.dumps(x) for x in response["body"]]
        requested_data = pd.DataFrame(
            {
                "data": rows,
                "_source_cnes": cnes,
                "_source_ap": endpoint_params["ap"],
                "_target_date": endpoint_params["target_date"],
                "_endpoint": endpoint_name,
                "_loaded_at": now,
            }
        )

        extracted_data.append(requested_data)

    return pd.concat(extracted_data)
