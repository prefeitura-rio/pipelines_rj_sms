# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import pytz
import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.cientificalab_v2.utils import (
    ensure_list,
    flatten_dict,
)
from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def authenticate_and_fetch(
    username: str, apccodigo: str, password: str, identificador_lis: str, dt_start: str, dt_end: str
) -> dict:

    auth_headers = {"emissor": username, "apccodigo": apccodigo, "pass": password}

    base_url = "https://cielab.lisnet.com.br/lisnetws"

    try:
        token_response = requests.get(f"{base_url}/tokenlisnet/apccodigo", headers=auth_headers)

        token_response.raise_for_status()

        token_data = token_response.json()

        if token_data["status"] != 200:
            message = f"(authenticate_and_fetch) Error getting token: {token_data['status']} - {token_data['mensagem']}"  # noqa
            raise Exception(message)

        token = token_data["token"]

        if not token:
            raise ValueError(
                "(authenticate_and_fetch) Authentication successful, but no token found in response"
            )  # noqa

        log("(authenticate_and_fetch) Authentication successful")

        results_headers = {"codigo": apccodigo, "token": token}

        request_body = {
            "lote": {
                "identificadorLis": identificador_lis,
                "dataResultado": {"inicial": dt_start, "final": dt_end},
                "parametros": {
                    "retorno": "ESTRUTURADO",
                    "parcial": "N",
                    "sigiloso": "S",
                },
            }
        }

        results_response = requests.post(
            f"{base_url}/APOIO/DTL/resultado", headers=results_headers, json=request_body
        )

        results_response.raise_for_status()

        results = results_response.json()

        if "status" in results["lote"] and results["lote"]["status"] != 200:
            message = f"(authenticate_and_fetch) Failed to get results: Status: {results['lote']['status']} Message: {results['lote']['mensagem']}"
            raise Exception(message)

        if "solicitacoes" not in results["lote"]:
            message = f"(authenticate_and_fetch) Failed to get results. No data available, message: {results['lote']['mensagem']}"
            raise Exception(message)

        log("(authenticate_and_fetch) Successfully fetched results", level="info")

        return results

    except Exception as e:
        error_message = str(e)
        log(f"(authenticate_and_fetch) Unexpected error: {error_message}", level="error")
        raise


@task(nout=3)
def transform(json_result: dict) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    solicitacoes_list = []
    exames_list = []
    resultados_list = []

    # timestamp para todas as tabelas
    now = datetime.now(pytz.timezone("America/Sao_Paulo"))

    lote_info = json_result.get("lote", {})
    lote_meta = {f"lote_{k}": v for k, v in lote_info.items() if k != "solicitacoes"}

    solicitacoes = ensure_list(lote_info.get("solicitacoes", {}).get("solicitacao", []))

    for solicitacao in solicitacoes:
        solicitacao_dict = {**solicitacao}

        paciente = solicitacao_dict.pop("paciente", {})
        resptec = solicitacao_dict.pop("responsaveltecnico", {})

        solicitacao_dict.update(flatten_dict(paciente, "paciente"))
        solicitacao_dict.update(flatten_dict(resptec, "responsaveltecnico"))
        solicitacao_dict.update(lote_meta)

        # Gerar ID determinístico
        id_solicitacao = str(
            uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{solicitacao.get('codigoLis','')}"
                f"{solicitacao.get('dataPedido','')}"
                f"{paciente.get('nome','')}"
                f"{paciente.get('cpf','')}",
            )
        )
        solicitacao_dict["id"] = id_solicitacao

        exames = ensure_list(solicitacao_dict.pop("exames", {}).get("exame", []))

        solicitacoes_list.append({**solicitacao_dict, "datalake_loaded_at": now})

        for exame in exames:
            exame_dict = {**exame}
            solicitante = exame_dict.pop("solicitante", {})
            exame_dict.update(flatten_dict(solicitante, "solicitante"))

            exame_dict["solicitacao_id"] = id_solicitacao
            id_exame = str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"{id_solicitacao}{exame.get('codigoApoio','')}{exame.get('amostraApoio','')}",
                )
            )
            exame_dict["id"] = id_exame

            resultados = ensure_list(exame_dict.pop("resultados", {}).get("resultado", []))
            exames_list.append({**exame_dict, "datalake_loaded_at": now})

            for resultado in resultados:
                resultado_dict = {**resultado}
                resultado_dict["exame_id"] = id_exame
                id_resultado = str(
                    uuid.uuid5(uuid.NAMESPACE_DNS, f"{id_exame}{resultado.get('codigoApoio','')}")
                )
                resultado_dict["id"] = id_resultado
                resultados_list.append({**resultado_dict, "datalake_loaded_at": now})

    solicitacoes_df = pd.DataFrame(solicitacoes_list)
    exames_df = pd.DataFrame(exames_list)
    resultados_df = pd.DataFrame(resultados_list)

    return solicitacoes_df, exames_df, resultados_df


@task
def generate_daily_windows(start_date: pd.Timestamp) -> List[Dict[str, str]]:
    """
    Gera janelas de extração diárias de 00:00:00 a 23:59:59
    """

    tz = pytz.timezone("America/Sao_Paulo")

    if start_date.tzinfo is None:
        start_date = tz.localize(start_date)

    # A data fim sempre vai ser ontem para pegar dia completo
    end_date = pd.Timestamp.now(tz) - pd.Timedelta(days=1)

    current_date = start_date.normalize()
    last_date = end_date.normalize()

    windows = []
    log(f"Gerando janelas de {current_date.date()} até {last_date.date()}")

    while current_date <= last_date:
        window_start = tz.localize(datetime.combine(current_date.date(), time.min))
        window_end = tz.localize(datetime.combine(current_date.date(), time.max))

        windows.append(
            {
                "dt_inicio": window_start.strftime("%Y-%m-%d %H:%M:%S%z"),
                "dt_fim": window_end.strftime("%Y-%m-%d %H:%M:%S%z"),
            }
        )

        current_date += pd.Timedelta(days=1)

    log("janelas geradas com sucesso.")
    return windows


@task
def get_identificador_lis(secret_json: str, cnes: str) -> str:
    data_dict = json.loads(secret_json)
    return data_dict[cnes]


@task
def build_operator_params(
    windows: List[Dict[str, str]],
    env: str,
    cnes_list: List[str],
) -> List[Dict[str, str]]:
    params = []

    for cnes in cnes_list:
        for window in windows:
            params.append(
                {
                    "cnes": cnes,
                    "dt_inicio": window["dt_inicio"],
                    "dt_fim": window["dt_fim"],
                    "environment": env,
                }
            )
    return params
