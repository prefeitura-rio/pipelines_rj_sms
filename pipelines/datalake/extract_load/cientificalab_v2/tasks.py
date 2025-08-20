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

        if token_data.get("status") != 200:
            message = f"Error getting token: {token_data.get('status')} - {token_data.get('mensagem')}"  # noqa
            raise Exception(message)

        token = token_data.get("token")

        if not token:
            raise ValueError("Authentication successful, but no token found in response")

        log("(authenticate_and_fetch) Token retrived successfully")

        results_headers = {"codigo": apccodigo, "token": token}

        request_body = {
            "lote": {
                "identificadorLis": identificador_lis,
                "dataResultado": {"inicial": dt_start, "final": dt_end},
                "parametros": {
                    "retorno": "ESTRUTURADO",
                    "parcial": "N",
                },
            }
        }

        results_response = requests.post(
            f"{base_url}/APOIO/DTL/resultado", headers=results_headers, json=request_body
        )

        results_response.raise_for_status()

        results = results_response.json()

        log("(authenticate_and_fetch) Successfully fetched results", level="info")

        return results

    except Exception as e:
        error_message = str(e)
        log(f"(authenticate_and_fetch) Unexpected error: {error_message}", level="error")
        raise


@task(nout=3)
def transform(resultado_json: dict) -> pd.DataFrame:
    solicitacoes_rows = []
    exames_rows = []
    resultados_rows = []

    lote = resultado_json.get("lote", {})
    lote_info = {f"lote_{k}": v for k, v in lote.items() if k != "solicitacoes"}

    solicitacoes = lote.get("solicitacoes", {}).get("solicitacao", [])
    if not isinstance(solicitacoes, list):
        solicitacoes = [solicitacoes]

    for s in solicitacoes:
        exames_data = s.pop("exames", {}).get("exame", [])
        if not isinstance(exames_data, list):
            exames_data = [exames_data]

        paciente_data = {f"paciente_{k}": v for k, v in s.pop("paciente", {}).items()}
        resp_tec_data = {
            f"responsaveltecnico_{k}": v for k, v in s.pop("responsaveltecnico", {}).items()
        }  # noqa

        solicitacao_row = {**s, **paciente_data, **resp_tec_data, **lote_info}
        solicitacao_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{solicitacao_row.get('codigoLis', '')}|{solicitacao_row.get('dataPedido', '')}|{solicitacao_row.get('paciente_nome', '')}", # noqa
            )
        )  # noqa
        solicitacao_row["id"] = solicitacao_id
        solicitacoes_rows.append(solicitacao_row)

        for e in exames_data:
            resultados_data = e.pop("resultados", {}).get("resultado", [])
            if not isinstance(resultados_data, list):
                resultados_data = [resultados_data]

            solicitante_data = {f"solicitante_{k}": v for k, v in e.pop("solicitante", {}).items()}

            exame_row = {**e, **solicitante_data}
            exame_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"{solicitacao_id}|{exame_row.get('codigoApoio', '')}|{exame_row.get('dataAssinatura', '')}", # noqa
                )
            )  # noqa
            exame_row["id"] = exame_id
            exame_row["solicitacao_id"] = solicitacao_id
            exames_rows.append(exame_row)

            for r in resultados_data:
                resultado_row = r.copy()
                resultado_row["resultado"] = resultado_row.get("resultado", "")
                resultado_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_DNS,
                        f"{exame_id}|{resultado_row.get('codigoApoio', '')}|{resultado_row.get('descricaoApoio', '')}", # noqa
                    )
                )  # noqa
                resultado_row["id"] = resultado_id
                resultado_row["exame_id"] = exame_id
                resultados_rows.append(resultado_row)

    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    solicitacoes_df = pd.DataFrame(solicitacoes_rows)
    exames_df = pd.DataFrame(exames_rows)
    resultados_df = pd.DataFrame(resultados_rows)

    for df in [solicitacoes_df, exames_df, resultados_df]:
        df["datalake_loaded_at"] = now

    exames_df = exames_df.drop(columns=["resultados"], errors="ignore")
    solicitacoes_df = solicitacoes_df.drop(
        columns=["paciente", "responsaveltecnico"], errors="ignore"
    )  # noqa

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
