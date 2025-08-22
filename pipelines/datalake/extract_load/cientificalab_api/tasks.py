# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime, time, timedelta
from typing import Dict, List

import pandas as pd
import pytz
import requests
from prefeitura_rio.pipelines_utils.logging import log

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
def transform(json_result: dict):

    solicitacoes_rows = []
    exames_rows = []
    resultados_rows = []

    lote = json_result.get('lote')

    if not lote:
        message = '(transform) lote not found in json response'
        raise ValueError(message)
    
    solicitacoes = lote.get('solicitacoes', {}).get('solicitacao', [])

    if isinstance(solicitacoes, dict):
        solicitacoes = [solicitacoes]

    lote_attrs = {f"lote_{k}": v for k, v in lote.items() if not isinstance(v, dict)}

    for solicitacao in solicitacoes:
        solicitacoes_row = {k: v for k, v in solicitacao.items() if not isinstance(v, (dict, list))}

        solicitacoes_row.update(lote_attrs)

        for entidade_suporte in ["responsaveltecnico", "paciente"]:
            nested_dict = solicitacao.get(entidade_suporte, {})
            for key, value in nested_dict.items():
                solicitacoes_row[f"{entidade_suporte}_{key}"] = value
        
        solicitacao_id = str(uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{solicitacoes_row.get('codigoLis', '')}|{solicitacoes_row.get('codigoApoio', '')}|"
            f"{solicitacoes_row.get('dataPedido', '')}|{solicitacoes_row.get('paciente_nome', '')}|"
            f"{solicitacoes_row.get('paciente_cpf', '')}"
        ))
        solicitacoes_row["id"] = solicitacao_id
        solicitacoes_rows.append(solicitacoes_row)

        exames = solicitacao.get('exames', {}).get('exame', [])

        if isinstance(exames, dict):
            exames = [exames]

        for exame in exames:
            exames_row = {k: v for k, v in exame.items() if not isinstance(v, (dict, list))}
            exames_row["solicitacao_id"] = solicitacao_id

            solicitante_dict = exame.get('solicitante', {})
            for key, value in solicitante_dict.items():
                exames_row[f"solicitante_{key}"] = value

            exame_id = str(uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{solicitacao_id}|{exames_row.get('codigoExame', '')}|"
                f"{exames_row.get('codigoApoio', '')}|{exames_row.get('dataAssinatura', '')}"
            ))
            exames_row["id"] = exame_id
            exames_rows.append(exames_row)

            resultados = exame.get('resultados', {}).get('resultado', [])

            if isinstance(resultados, dict):
                resultados = [resultados]

            for resultado in resultados:
                resultados_row = {k: v for k, v in resultado.items()}
                resultados_row["exame_id"] = exame_id
                
                resultado_id = str(uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"{resultados_row.get('codigoApoio', '')}|"
                    f"{resultados_row.get('descricaoApoio', '')}|"
                    f"{exame_id}"
                ))
                resultados_row["id"] = resultado_id
                resultados_rows.append(resultados_row)
        
    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    solicitacoes_df = pd.DataFrame(solicitacoes_rows)
    exames_df = pd.DataFrame(exames_rows)
    resultados_df = pd.DataFrame(resultados_rows)

    for df in [solicitacoes_df, exames_df, resultados_df]:
        if not df.empty:
            df["datalake_loaded_at"] = now

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
