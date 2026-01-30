# -*- coding: utf-8 -*-
import pytz
import requests
from pandas import DataFrame
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from google.cloud import bigquery
from pipelines.datalake.extract_load.siclom_api.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


def make_request(url: str, headers: dict) -> requests.Response | None:
    """Faz requisição para uma API utilizando o método GET."""
    session = requests.Session()
    retries = Retry(total=2, backoff_factor=1, status_forcelist=[502, 503, 504, 104])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    try:
        response = session.get(url=url, headers=headers)
    except requests.exceptions.RequestException:
        return None
    return response

@task
def format_month(month:str, year:str) -> str:
    """Formata string numérica referente ao mês-ano."""
    if int(month) > 12:
        raise Exception(msg="Mês inválido.")
    return f"{str(month).zfill(2)}/{year}"

@task
def generate_formated_months(reference_year:str, interval) -> list[str]:
    """Gera strings numéricas formatadas referente aos meses para extrações anuais."""
    
    periods = []
    for year in range(reference_year, reference_year - interval, -1):
        periods.extend([
            f"{str(month).zfill(2)}/{year}" for month in range(1, 13)
            ])
    log(periods)
    return periods

@task
def get_siclom_period_data(
    endpoint: str, 
    api_key: str, 
    period: str
    ) -> DataFrame:
    """Faz a requisição para a API do SICLOM utilizando a busca por mês e ano."""
    log(f"Buscando dados de {period}...")
    
    headers = {"Accept": "application/json", "X-API-KEY": api_key}
    
    base_url = constants.BASE_URL.value
    url = f"{base_url}{endpoint}{period}"
    
    response = make_request(url=url, headers=headers)
    
    if response and response.status_code == 200:
       data = response.json()['resultado']
       df = DataFrame(data)
       df['extracted_at'] = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')
       
       log('✅ Extração realizada com sucesso!')
       return df
    
    else:
       return DataFrame()

# Após a implementação do endpoint para obter dados por período na API do SICLOM, 
# as seguintes tasks perderam o sentido em serem utilizadas, mas preferi mante-las
# para possíveis necessidades futuras.
@task
def get_patient_data(
    environment: str,
    table_id, 
    batch: int, 
    retry: bool
    ):
    """
    Obtém os CPFs a serem consultados na API do SICLOM e divide em lotes.

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
def fetch_siclom_data(cpf_batch: list, endpoint: str, api_key: str):
    """
    Faz a requisição para a API do SICLOM utilizando a busca por CPF.
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
        df = DataFrame(patients_data)
        df["extracted_at"] = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    else:
        df = DataFrame()

    return df