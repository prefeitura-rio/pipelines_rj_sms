# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
import pytz
from google.cloud import bigquery

from pipelines.datalake.extract_load.siclom_api.constants import constants
from pipelines.datalake.extract_load.siclom_api.utils import make_request
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def get_patient_data(
    environment: str,
    table_id, 
    batch: int, 
    retry: bool
    ):
    """
    Obtém os CPFs a serem consultados na API do SICLOM.

    Parâmetros:
        - `batch`: Tamanho de cada lote de CPFs a ser gerado para requisições paralelizadas
        - `table_id`: Tabela de destino dos dados.
        - `retry`: Utilizada para obter CPFs que não estão na tabela fim. Essa flag pode ser utilizada 
        tanto para obter novos CPFs ou para possíveis CPFs que tiveram erro durante a extração pela API.
    """
    
    log("Obtendo dados dos pacientes de interesse...")
    client = bigquery.Client()
    
    if retry:
        # TODO: ALTERAR PARA O DATASET DE PRODUÇÃO
        sql = f"""
                with 
                    pacientes_hci as (
                        select distinct paciente_cpf as cpf
                        from `rj-sms.saude_historico_clinico.episodio_assistencial`
                        where `condicoes`[SAFE_OFFSET(0)].id  in ('B200','B204','B222','B217','B20','B205',
                        'B238','Z21','B211','B213','Z830','B208','Z114','B231','B203','B201','Z206','B212',
                        'B221','B24','B209','B220','B219','B210','B230','B207','B21','B22','F024','B232',
                        'B23','B206','Z717','R75','B218','B202','B227')
                    )
                select distinct cpf 
                from pacientes_hci 
                where cpf not in (select cpf from rj-sms-dev.Herian__brutos_siclom_api.{table_id})       
        """
    else:
        # TODO: Confirmar com a SAP qual a melhor tabela para obter os CPFs de interesse
        sql = """
                select distinct paciente_cpf as cpf
                from `rj-sms.saude_historico_clinico.episodio_assistencial`
                where `condicoes`[SAFE_OFFSET(0)].id  in ('B200','B204','B222','B217','B20','B205','B238','Z21',
                        'B211','B213','Z830','B208','Z114','B231','B203','B201','Z206','B212','B221',
                        'B24','B209','B220','B219','B210','B230','B207','B21','B22','F024','B232',
                        'B23','B206','Z717','R75','B218','B202','B227')
        """

    df = client.query_and_wait(sql).to_dataframe()
    cpf_list = df["cpf"].to_list()
    chunks = [cpf_list[i : i + batch] for i in range(0, len(cpf_list), batch)]

    return chunks

@task
def fetch_siclom_data(cpf_batch, endpoint, api_key):
    """
    Faz a requisição para a API do SICLOM.

    Args:
        `environment` (_str_): Ambiente de execução do flow.
        `cpf_batch` (_list_): Lote de CPFs a serem consultados na API.
        `endpoint` (_str_): Endpoint da API.
        `api_key` (_str_): Chave de autenticação da API.

    Returns:
        `pd.DataFrame`: _description_
    """
    log(f"Buscando dados do Siclom para o lote de CPFs...")

    base_url = constants.BASE_URL.value
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json", "X-API-KEY": api_key}

    patients_data = []

    for cpf in cpf_batch:
        try:
            response = make_request(url=url + cpf, headers=headers)
            if not response:
                continue  # Atualmente a API retorna 500 quando o CPF não é encontrado.
            if response.status_code == 200:
                patients_data.extend(response.json()["resultado"])
        except TypeError:
            # Casos que por algum motivo o CPF é considerado None
            continue
        except Exception as e:
            raise e
    if patients_data:
        df = pd.DataFrame(patients_data)
        df["extracted_at"] = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    else:
        df = pd.DataFrame()

    return df
