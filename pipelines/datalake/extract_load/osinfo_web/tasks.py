# -*- coding: utf-8 -*-
"""
OSInfo dumping tasks
"""
from typing import List
import requests
import pandas as pd

from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.datalake.extract_load.osinfo_web.constants import constants as osinfo_constants


@task()
def download_from_osinfo(destination_folder: str, endpoint: str = "despesa"):
    """"""

    if endpoint == "despesa":
        header = osinfo_constants.OSINFO_HEADER.value["despesa"]
    else:
        raise ValueError(f"Invalid endpoint: {endpoint}")

    url = "https://osinfo.prefeitura.rio/expenses/server/expensesService/expensesToXls"

    requisicao_despesas = {
        "cod_os": 1,
        "ref_mes": 6,
        "ref_ano": 2024,
        "propria_os": "N",
        "redesenhar": 3,
        "tamanho": -1,
        "tamanho_anterior": -1,
        "campo_ordenacao": "id_documento",
        "direcao": "ASC",
        "pagina": 1,
        "pesquisa_global": "",
        "pesquisa_coluna": '{"descricaoRubrica":"642"}',
    }

    requisitar = requests.post(url, json=requisicao_despesas, timeout=60)

    resultado = requisitar.json()
    log(f"Extraindo dados para o mês {6} de {2024}. Status: {requisitar.status_code}", level="info")

    df = pd.DataFrame(columns=resultado[0], data=resultado[1:-1])

    log("Sucesso na extração dos dados", level="info")

    # salvar arquivo com final YYYY-MM
    destination_file = f"{destination_folder}/meu_arquivo.parquet"


    df.to_parquet(destination_file, index=False)
    log(f"Arquivo salvo em {destination_folder}", level="info")

    return [destination_file]


@task()
def transform_data(files_path: List[str]):

    transformed_files = []

    for file in files_path:
        # Manter a colunas
        # qualquer transformação
        transformed_files.append(conform_header_to_datalake(file_path=file, file_type="parquet"))
    
    return transformed_files
