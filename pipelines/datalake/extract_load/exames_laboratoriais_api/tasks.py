# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime, timedelta

import pandas as pd
import pytz
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.exames_laboratoriais_api.constants import (
    AREA_PROGRAMATICA,
    CREDENTIALS,
)

from pipelines.datalake.extract_load.exames_laboratoriais_api.utils import send_api_error_report

from pipelines.utils.credential_injector import authenticated_task as task
import requests



@task(max_retries=3, retry_delay=timedelta(minutes=1))
def authenticate(
    username: str,
    apccodigo: str,
    password: str,
    source: str,
) -> dict:
    """
    Realiza apenas a autenticação e retorna o token com o timestamp de criação.
    """
    auth_headers = {"emissor": username, "apccodigo": apccodigo, "pass": password}

    if source == "cientificalab":
        base_url = "https://cielab.lisnet.com.br/lisnetws"
    else:
        base_url = "https://biomega-api.lisnet.com.br/lisnetws"

    token_response = requests.get(
        f"{base_url}/tokenlisnet/apccodigo",
        headers=auth_headers,
    )

    token_data = token_response.json()

    if token_data.get("status") != 200:
        message = f"(authenticate) Erro ao obter token: {token_data.get('mensagem')}"
        raise Exception(message)

    log("Autenticação realizada com sucesso.")

    return {
        "token": token_data["token"],
        "base_url": base_url,
        "created_at": datetime.now(tz=pytz.timezone("America/Sao_Paulo")).timestamp(),
    }


def fetch_window(
    auth: dict,
    apccodigo: str,
    identificador_lis: str,
    dt_start: str,
    dt_end: str,
    source: str,
    environment: str,
) -> dict:
    """
    Busca os dados de uma janela de tempo usando um token já existente.
    Função simples (não é task) — será chamada dentro do loop do Operator.
    """
    token = auth["token"]
    base_url = auth["base_url"]

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

    results_response = requests.post(
        f"{base_url}/APOIO/DTL/resultado",
        headers=results_headers,
        json=request_body,
    )

    if results_response.status_code in [502, 503]:
        message = (
            f"(fetch_window) Service Unavailable (Status {results_response.status_code}). "
            "Possível manutenção ou instabilidade na API"
        )
        log(message, level="warning")

        send_api_error_report(
            status_code=results_response.status_code,
            source=source,
            environment=environment,
        )

        return {"lote": {"status": results_response.status_code, "mensagem": "API Fora do Ar"}}

    results = results_response.json()

    if isinstance(results, str):
        error_message = f"(fetch_window) request falhou: {results}"
        log(error_message, level="error")
        raise Exception(error_message)

    if "lote" in results and results["lote"].get("status") != 200:
        lote_status = results["lote"].get("status")
        lote_mensagem = results["lote"].get("mensagem")

        if (
            lote_status == 501
            and "Resultado não disponíveis para data solicitada" in lote_mensagem
        ):
            log(f"(fetch_window) Status 501: {lote_mensagem}", level="warning")
            return results

        elif lote_status is not None and lote_status != 200:
            message = f"(fetch_window) Falha ao buscar resultados: Status: {lote_status} Mensagem: {lote_mensagem}"
            raise Exception(message)

    return results

@task(nout=3, max_retries=3, retry_delay=timedelta(minutes=1))
def process_all_windows(
    windows: list,
    username: str,
    apccodigo: str,
    password: str,
    identificador_lis: str,
    source: str,
    environment: str,
) -> tuple:
    """
    Processa todas as janelas de uma AP em sequência, reaproveitando o token
    e reautenticando automaticamente quando necessário.
    Retorna os DataFrames acumulados de todas as janelas.
    """
    TOKEN_TTL = 420  # 480s de validade, usamos 420 pra ter margem de segurança

    solicitacoes_acc = []
    exames_acc = []
    resultados_acc = []

    # Autenticação inicial
    auth = authenticate.run(
        username=username,
        apccodigo=apccodigo,
        password=password,
        source=source,
    )

    for i, window in enumerate(windows):
        dt_start = window["dt_inicio"]
        dt_end = window["dt_fim"]

        # Verifica se o token ainda é válido, reautentica se necessário
        elapsed = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).timestamp() - auth["created_at"]
        if elapsed >= TOKEN_TTL:
            log(f"Token expirado após {elapsed:.0f}s. Reautenticando...")
            auth = authenticate.run(
                username=username,
                apccodigo=apccodigo,
                password=password,
                source=source,
            )

        log(f"[{i+1}/{len(windows)}] Buscando janela {dt_start} → {dt_end}")

        try:
            result = fetch_window(
                auth=auth,
                apccodigo=apccodigo,
                identificador_lis=identificador_lis,
                dt_start=dt_start,
                dt_end=dt_end,
                source=source,
                environment=environment,
            )
        except Exception as e:
            log(f"Erro na janela {dt_start} → {dt_end}: {str(e)}. Pulando.", level="warning")
            continue

        # Pula janelas sem dados (status 501 ou API fora do ar)
        lote = result.get("lote", {})
        lote_status = lote.get("status")
        if lote_status in [501, 502, 503]:
            log(f"Janela {dt_start} → {dt_end} sem dados (status {lote_status}). Pulando.")
            continue

        solicitacoes_df, exames_df, resultados_df = transform.run(
            json_result=result,
            source=source,
        )

        if not solicitacoes_df.empty:
            solicitacoes_acc.append(solicitacoes_df)
        if not exames_df.empty:
            exames_acc.append(exames_df)
        if not resultados_df.empty:
            resultados_acc.append(resultados_df)

    log("Todas as janelas processadas. Consolidando DataFrames...")

    solicitacoes_final = pd.concat(solicitacoes_acc, ignore_index=True) if solicitacoes_acc else pd.DataFrame()
    exames_final = pd.concat(exames_acc, ignore_index=True) if exames_acc else pd.DataFrame()
    resultados_final = pd.concat(resultados_acc, ignore_index=True) if resultados_acc else pd.DataFrame()

    log(
        f"Totais acumulados → solicitacoes: {len(solicitacoes_final)}, "
        f"exames: {len(exames_final)}, resultados: {len(resultados_final)}"
    )

    return solicitacoes_final, exames_final, resultados_final


@task(nout=3)
def transform(json_result: dict, source: str):

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

    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

    solicitacoes_df = pd.DataFrame(solicitacoes_rows)
    exames_df = pd.DataFrame(exames_rows)
    resultados_df = pd.DataFrame(resultados_rows)

    for df in [solicitacoes_df, exames_df, resultados_df]:
        if not df.empty:
            df["datalake_loaded_at"] = now
            df["source"] = source

    return solicitacoes_df, exames_df, resultados_df


@task
def parse_identificador(identificador: str, ap: str) -> str:
    try:
        identificador_corrigido = identificador.replace("“", '"').replace("”", '"')
        identificador_dict = json.loads(identificador_corrigido)
        key = f"AP{ap}"
        identificador_lis = identificador_dict.get(key) or identificador_dict.get(ap)
        return identificador_lis
    except (json.JSONDecodeError, TypeError, AttributeError):
        return identificador


@task
def get_credential_param(source: str):
    return CREDENTIALS[source]


@task
def get_source_from_ap(ap: str) -> str:
    source = AREA_PROGRAMATICA.get(ap)

    if not source:
        message = f"AP {ap} não encontrada no mapeamento AREA_PROGRAMATICA."
        log(message, level="error")
        raise ValueError(message)

    return source


@task
def get_all_aps():
    return list(AREA_PROGRAMATICA.keys())


@task
def generate_time_windows(start_date: pd.Timestamp, end_date: str, hours_per_window: int = 2):
    """
    Gera janelas de tempo de 2 horas desde a start_date até a meia-noite de ontem
    """
    tz = pytz.timezone("America/Sao_Paulo")

    if start_date.tzinfo is None:
        start_date = tz.localize(start_date)

    # A data fim sempre vai ser a meia-noite do dia anterior (se n for passada)
    # Ex: Se hoje é 24/11 12h -> end_date é 24/11 00:00:00
    end_date_limit = pd.Timestamp.now(tz).normalize()

    if end_date:
        end_date_limit = pd.to_datetime(end_date).tz_localize(tz).normalize()
    else:
        end_date_limit = pd.Timestamp.now(tz).normalize()

    # 1. GERAÇÃO DOS PONTOS DE INÍCIO DA JANELA
    # Cria uma sequência de datas de 'start_date' até 'end_date_limit', com o passo de 'Xh'
    start_points = pd.date_range(
        start=start_date, end=end_date_limit, freq=f"{hours_per_window}H", inclusive="left"
    )

    windows = []

    # 2. CRIAÇÃO DAS JANELAS
    for window_start in start_points:
        window_end = window_start + timedelta(hours=hours_per_window, seconds=-1)

        # Garante que a última janela não ultrapasse o limite de ontem à meia-noite
        if window_end >= end_date_limit:
            continue

        windows.append(
            {
                # Garnte o formato com timezone
                "dt_inicio": window_start.strftime("%Y-%m-%d %H:%M:%S%z"),
                "dt_fim": window_end.strftime("%Y-%m-%d %H:%M:%S%z"),
            }
        )

    log(
        f"{len(windows)} janelas geradas com sucesso de {start_date.date()} até {end_date_limit.date()}"
    )
    return windows


@task
def build_operator_params(windows: list, aps: list, env: str, dataset: str):
    """
    Cria a lista de dicionários de parâmetros para o flow operator.
    Para cada AP, passa todas as janelas de tempo de uma vez.
    """
    params = []

    for ap in aps:
        params.append(
            {
                "windows": windows,
                "ap": ap,
                "environment": env,
                "dataset": dataset,
            }
        )

    log(f"Parâmetros gerados para {len(params)} APs, cada uma com {len(windows)} janelas.")
    return params
