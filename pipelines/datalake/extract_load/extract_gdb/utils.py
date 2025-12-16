# -*- coding: utf-8 -*-
import datetime
import re
import tempfile
from typing import List, Literal, Optional, Union

import pandas as pd
import pytz
import requests
from google.cloud import storage

from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key

from . import shared
from .constants import constants as flow_constants


def get_bearer_token(environment: str = "dev") -> str:
    # Se já temos um token salvo
    if "token" in shared.token and "expires" in shared.token:
        # Verifica se o token ainda está ativo
        expires_in = shared.token["expires"]
        now = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        # Se sim, retorna ele mesmo
        if now < expires_in:
            log("Reusing previous access token")
            return shared.token["token"]
        log("Access token expired; obtaining new one")
    # Se não, precisamos obter um token novo

    username = get_secret_key.run(
        environment=environment, secret_name="USERNAME", secret_path="/gdb-extractor"
    )
    password = get_secret_key.run(
        environment=environment, secret_name="PASSWORD", secret_path="/gdb-extractor"
    )

    base_url = flow_constants.API_URL.value
    resp = requests.post(f"{base_url}/token", data={"username": username, "password": password})
    resp.raise_for_status()
    json = resp.json()
    if "access_token" not in json:
        log(json)
        raise ValueError("No access token!")

    # Tempo de expiração previsto para o token subtraído de 30s
    seconds_left = max(0, json["expires_in"] - 30)
    log(f"Access token obtained; expires in {(seconds_left/60):.1f} min")
    # Salva data/hora de expiração
    shared.token["expires"] = datetime.datetime.now(
        tz=pytz.timezone("America/Sao_Paulo")
    ) + datetime.timedelta(seconds=seconds_left)
    # Pega token e salva para reuso
    token = json["access_token"]
    shared.token["token"] = token
    return token


def authenticated_request(
    method: Union[Literal["GET"], Literal["POST"]],
    endpoint: str,
    json: Optional[dict],
    enviroment: str = "dev",
) -> dict:
    token = get_bearer_token(environment=enviroment)

    base_url = flow_constants.API_URL.value
    resp = requests.request(
        method, f"{base_url}{endpoint}", json=json, headers={"Authorization": f"Bearer {token}"}
    )
    # Possível que o token salvo seja inválido (ex. servidor reiniciou no meio tempo)
    if resp.status_code == 401:
        # Limpa 'cache'
        shared.token = dict()
        # Obtém novo token
        token = get_bearer_token(environment=enviroment)
        # Tenta requisição mais uma vez
        resp = requests.request(
            method,
            f"{base_url}{endpoint}/",
            json=json,
            headers={"Authorization": f"Bearer {token}"},
        )
    resp.raise_for_status()
    return resp.json()


def authenticated_post(endpoint: str, json: dict, enviroment: str = "dev") -> dict:
    return authenticated_request("POST", endpoint, json, enviroment=enviroment)


def authenticated_get(endpoint: str, enviroment: str = "dev") -> dict:
    return authenticated_request("GET", endpoint, None, enviroment=enviroment)


def inverse_exponential_backoff(iteration: int, initial: int, base: int, minimum: int):
    return max(initial / (base**iteration), minimum)


def download_gcs_to_file(gcs_uri: str):
    # gcs_uri deve ser algo como "gs://bucket/path/to/your/file.txt"
    path_parts = gcs_uri.removeprefix("gs://").split("/", maxsplit=1)
    bucket_name = path_parts[0]
    blob_name = path_parts[1] if len(path_parts) > 1 else ""
    if not len(blob_name):
        raise ValueError(f"Invalid URI (no file path): '{gcs_uri}'")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    file_hdl = tempfile.TemporaryFile()
    blob.download_to_file(file_hdl)
    return file_hdl


def format_reference_date(refdate: str | None, uri: str) -> str:
    # Se não há data especificada
    if refdate is None or not refdate:
        # Tentamos pegar do nome do arquivo
        # Os arquivos normalmente terminam ou com MMYYYY ou YYYYMM

        # gs://path/to/file.name.GDB -> [ gs://path/to, file.name.GDB ][1]
        # -> file.name.GDB -> [file.name, GDB][0] -> file.name
        filename = uri.rsplit("/", maxsplit=1)[1].rsplit(".", maxsplit=1)[0]
        # Se o arquivo termina com 6 dígitos
        if re.search(r"[0-9]{6}$", filename):
            # Pegamos somente os dígitos
            dt = filename[-6:]
            # Se os dois primeiros são > 12, então a data começa com ano
            if int(dt[:2]) > 12:
                # [YYYY]MM
                year = dt[:4]
                # YYYY[MM]
                month = dt[4:]
            # Senão, começa com <= 12, e não estamos em um ano <1200
            # Então o mês vem primeiro
            else:
                # [MM]YYYY
                month = dt[:2]
                # MM[YYYY]
                year = dt[2:]

            # TODO: ajustar até 12/dez/2099
            if 2000 < int(year) < 2100 and 0 < int(month) < 13:
                return f"{year}-{month}-01"
        # Aqui, o arquivo não termina com mês e ano; não temos
        # ideia de qual data ele se refere, e não recebemos
        # `refdate` como parâmetro. Então a melhor opção é a
        # data atual :/
        dt = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        return dt.strftime("%Y-%m-01")

    # Aqui, recebemos data de referência! Uhules
    refdate = str(refdate).strip()
    # YYYY-MM?-DD?
    if re.search(r"^[0-9]{4}$", refdate):
        return f"{refdate}-01-01"
    if re.search(r"^[0-9]{4}[\-\/][0-9]{2}$", refdate):
        return f"{refdate}-01".replace("/", "-")
    if re.search(r"^[0-9]{4}[\-\/][0-9]{2}[\-\/][0-9]{2}$", refdate):
        return f"{refdate}".replace("/", "-")
    raise ValueError(f"Expected reference date format YYYY(-MM(-DD)?)?; got '{refdate}'")


def jsonify_dataframe(df: pd.DataFrame, keep_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Convert all rows of a dataframe into JSON strings.
    This may affect the object referenced by `df`; make a copy before using this
    function if you need to use its previous value.

    Args:
        df (pd.DataFrame):
            Source dataframe.
        keep_columns (List[str]?):
            Optional list of column names to leave out of JSON value.
            If not specified, the resulting dataframe will have a single
            column, 'json'. If a string `s` is passed instead of a list,
            this will be equivalent to `keep_columns=[s]`. If there are
            no columns outside of the list in `keep_columns`, an empty-string
            `"json"` column will be added.

    Returns:
        df (DataFrame):
            Modified dataframe.

    >>> a = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6], 'col3': [7, 8, 9]})
    >>> jsonify_dataframe(a)  # equivalent to keep_columns=[]
                               json
    0  {"col1":1,"col2":4,"col3":7}
    1  {"col1":2,"col2":5,"col3":8}
    2  {"col1":3,"col2":6,"col3":9}
    >>> jsonify_dataframe(a, keep_columns="col1")
                      json  col1
    0  {"col2":4,"col3":7}     1
    1  {"col2":5,"col3":8}     2
    2  {"col2":6,"col3":9}     3
    >>> jsonify_dataframe(a, keep_columns=["col1", "col3"])
             json  col1  col3
    0  {"col2":4}     1     7
    1  {"col2":5}     2     8
    2  {"col2":6}     3     9
    >>> jsonify_dataframe(a, keep_columns=["col1", "col2", "col3"])
      json  col1  col2  col3
    0   {}     1     4     7
    1   {}     2     5     8
    2   {}     3     6     9
    """
    keep_columns = keep_columns or []
    if isinstance(keep_columns, str):
        keep_columns = [keep_columns]

    # Colunas que queremos inserir no JSON
    json_columns = set(df.columns) - set(keep_columns)
    if len(json_columns) <= 0:
        df.insert(0, "json", "{}")
        return df

    # Aqui é dupla negativa: estamos REMOVENDO colunas que NÃO vão pro JSON
    # i.e. mantemos somente colunas que VÃO pro JSON
    df_json = df.drop(columns=list(keep_columns))
    # Oposto aqui: removemos colunas do JSON, i.e. mantemos as fora do JSON
    df_keep = df.drop(columns=list(json_columns))
    # Transforma colunas em JSON
    df_json["json"] = df_json.to_json(orient="records", lines=True).splitlines()
    # Remove colunas que foram pro JSON (então só resta a coluna do JSON)
    df_json.drop(columns=list(json_columns), inplace=True)
    # Merge do novo dataframe de JSON com as colunas anteriores
    # Em teoria teremos o mesmo número de linhas, na mesma ordem, ...
    _tmp_df = pd.merge(df_json, df_keep, left_index=True, right_index=True)
    return _tmp_df
