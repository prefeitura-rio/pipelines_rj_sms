# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import pytz
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import cloud_function_request


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def authenticate_and_fetch(
    username: str,
    apccodigo: str,
    password: str,
    identificador_lis: str,
    dt_start: str,
    dt_end: str,
    environment: str,
) -> dict:

    auth_headers = {"emissor": username, "apccodigo": apccodigo, "pass": password}

    base_url = "https://cielab.lisnet.com.br/lisnetws"

    try:
        token_response = cloud_function_request.run(
            url=f"{base_url}/tokenlisnet/apccodigo",
            request_type="GET",
            header_params=auth_headers,
            api_type="json",
            env=environment,
            endpoint_for_filename="cientificalab_token",
            credential=None,
        )

        token_data = token_response["body"]
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
                    "retorno": "ESTRUTURADO/LINK",
                    "parcial": "N",
                    "sigiloso": "S",
                },
            }
        }

        results_response = cloud_function_request.run(
            url=f"{base_url}/APOIO/DTL/resultado",
            request_type="POST",
            header_params=results_headers,
            body_params=request_body,
            api_type="json",
            env=environment,
            endpoint_for_filename="cientificalab_results",
            credential=None,
        )

        results = results_response["body"]

        if "status" in results["lote"] and results["lote"]["status"] != 200:
            message = f"(authenticate_and_fetch) Failed to get results: Status: {results['lote']['status']} Message: {results['lote']['mensagem']}"  # noqa
            raise Exception(message)

        if "solicitacoes" not in results["lote"]:
            message = f"(authenticate_and_fetch) Failed to get results. No data available, message: {results['lote']['mensagem']}"  # noqa
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

    lote = json_result.get("lote")

    if not lote:
        message = "(transform) lote not found in json response"
        raise ValueError(message)

    solicitacoes = lote.get("solicitacoes", {}).get("solicitacao", [])

    if isinstance(solicitacoes, dict):
        solicitacoes = [solicitacoes]

    lote_attrs = {f"lote_{k}": v for k, v in lote.items() if not isinstance(v, dict)}

    for solicitacao in solicitacoes:
        solicitacoes_row = {k: v for k, v in solicitacao.items() if not isinstance(v, (dict, list))}

        solicitacoes_row.update(lote_attrs)

        # cria os campos com o prefixo da key. EX: paciente_nome
        for entidade_suporte in ["responsaveltecnico", "paciente"]:
            nested_dict = solicitacao.get(entidade_suporte, {})
            for key, value in nested_dict.items():
                solicitacoes_row[f"{entidade_suporte}_{key}"] = value

        solicitacao_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{solicitacoes_row.get('codigoLis', '')}|"
                f"{solicitacoes_row.get('codigoApoio', '')}|"
                f"{solicitacoes_row.get('dataPedido', '')}|"
                f"{solicitacoes_row.get('paciente_nome', '')}|"
                f"{solicitacoes_row.get('codunidade', '')}|"
                f"{solicitacoes_row.get('origem', '')}|"
                f"{solicitacoes_row.get('paciente_cpf', '')}",
            )
        )
        solicitacoes_row["id"] = solicitacao_id
        solicitacoes_rows.append(solicitacoes_row)

        exames = solicitacao.get("exames", {}).get("exame", [])

        if isinstance(exames, dict):
            exames = [exames]

        for exame in exames:
            exames_row = {k: v for k, v in exame.items() if not isinstance(v, (dict, list))}
            exames_row["solicitacao_id"] = solicitacao_id

            # cria os campos com o prefixo da key. EX: solicitante_nome
            solicitante_dict = exame.get("solicitante", {})
            for key, value in solicitante_dict.items():
                exames_row[f"solicitante_{key}"] = value

            exame_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"{solicitacao_id}|{exames_row.get('codigoExame', '')}|"
                    f"{exames_row.get('codigoApoio', '')}|{exames_row.get('dataAssinatura', '')}",
                )
            )
            exames_row["id"] = exame_id
            exames_rows.append(exames_row)

            resultados = exame.get("resultados", {}).get("resultado", [])

            if isinstance(resultados, dict):
                resultados = [resultados]

            for resultado in resultados:
                resultados_row = {k: v for k, v in resultado.items()}
                resultados_row["exame_id"] = exame_id

                resultado_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_DNS,
                        f"{resultados_row.get('codigoApoio', '')}|"
                        f"{resultados_row.get('descricaoApoio', '')}|"
                        f"{exame_id}",
                    )
                )
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
def generate_time_windows(
    start_date: pd.Timestamp, hours_per_window: int = 2
) -> List[Dict[str, str]]:
    """
    Gera janelas de extração de X horas a partir de uma data de início.
    Ex: Para 4 horas, gera janelas de 00:00-03:59, 04:00-07:59, etc.
    """
    tz = pytz.timezone("America/Sao_Paulo")

    if start_date.tzinfo is None:
        start_date = tz.localize(start_date)

    # A data fim sempre vai ser ontem para pegar o último dia completo
    end_date = pd.Timestamp.now(tz) - pd.Timedelta(days=1)

    # Itera dia a dia, do início ao fim
    current_day = start_date.normalize()
    last_day = end_date.normalize()

    windows = []
    log(
        f"Gerando janelas de {hours_per_window} horas de {current_day.date()} até {last_day.date()}"
    )  # noqa

    # Loop principal para iterar sobre os dias
    while current_day <= last_day:
        # Loop secundário para criar as janelas dentro de um dia
        for hour_start in range(0, 24, hours_per_window):
            window_start = current_day.replace(hour=hour_start, minute=0, second=0)

            # O fim da janela é X horas depois, menos 1 segundo
            window_end = window_start + timedelta(hours=hours_per_window, seconds=-1)

            windows.append(
                {
                    "dt_inicio": window_start.strftime("%Y-%m-%d %H:%M:%S%z"),
                    "dt_fim": window_end.strftime("%Y-%m-%d %H:%M:%S%z"),
                }
            )

        # Avança para o próximo dia
        current_day += timedelta(days=1)

    log(f"{len(windows)} janelas geradas com sucesso.")
    return windows


@task
def build_operator_params(
    windows: List[Dict[str, str]], identificadores: List[str], env: str
) -> List[Dict[str, str]]:
    params = []

    for window in windows:
        for identificador in identificadores:
            params.append(
                {
                    "dt_inicio": window["dt_inicio"],
                    "dt_fim": window["dt_fim"],
                    "identificador_lis": identificador,
                    "environment": env,
                }
            )
    return params


@task
def parse_identificador(identificador: str) -> List[str]:
    identificador_corrigido = identificador.replace("“", '"').replace("”", '"')
    identificador_dict = json.loads(identificador_corrigido)
    return list(identificador_dict.values())
