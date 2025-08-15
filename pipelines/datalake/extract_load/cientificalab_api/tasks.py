# -*- coding: utf-8 -*-
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import pytz
from bs4 import BeautifulSoup
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.cientificalab_api.constants import (  # noqa
    cientificalab_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import cloud_function_request
from pipelines.utils.time import get_datetime_working_range


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def authenticate_and_fetch(
    username: str,
    password: str,
    apccodigo: str,
    identificador_lis: str,
    dt_inicio: str = "2025-01-21T10:00:00-0300",
    dt_fim: str = "2025-01-21T11:30:00-0300",
    environment: str = "dev",
):
    token_response = cloud_function_request.run(
        url="https://cielab.lisnet.com.br/lisnetws/tokenlisnet/apccodigo",
        request_type="GET",
        query_params={
            "Content-Type": "application/json",
            "apccodigo": apccodigo,
            "emissor": username,
            "pass": password,
        },
        body_params="",
        api_type="xml",
        env=environment,
        credential=None,
        endpoint_for_filename="lab_token",
    )

    if token_response.get("status_code") != 200:
        message = f"Failed to get token from Lisnet API: {token_response.get('status_code')} - {token_response.get('body')}"  # noqa
        raise Exception(message)

    token_data_string = token_response.get("body")
    token_data = json.loads(token_data_string)

    if token_data.get("status") != 200:
        message = f"Lisnet API returned error for token: {token_data.get('status')} - {token_data.get('mensagem')}"  # noqa
        raise Exception(message)

    token = token_data.get("token")

    data = f"""
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <lote>
            <codigoLis></codigoLis>
            <identificadorLis>{identificador_lis}</identificadorLis>
            <origemLis>1</origemLis>
            <dataResultado>
                <inicial>{dt_inicio}</inicial>
                <final>{dt_fim}</final>
            </dataResultado>
            <parametros>
                <parcial>N</parcial>
                <retorno>ESTRUTURADO</retorno>
            </parametros>
        </lote>
        """.strip()

    resultado_response = cloud_function_request.run(
        url="https://cielab.lisnet.com.br/lisnetws/APOIO/DTL/resultado",
        request_type="GET",
        query_params={
            "codigo": apccodigo,
            "token": token,
        },
        body_params=data,
        api_type="xml",
        env=environment,
        credential=None,
        endpoint_for_filename="lab_exames",
    )

    if resultado_response.get("status_code") != 200:
        message = f"Failed to get XML results from Lisnet API: {resultado_response.get('status_code')} - {resultado_response.get('body')}"  # noqa
        raise Exception(message)

    resultado_xml = resultado_response["body"]

    if (
        "Resultado não disponíveis para data solicitada" in resultado_xml
        or "<solicitacoes>" not in resultado_xml
    ):
        log(f"Resultado não encontrado {resultado_xml}", level="error")
        raise Exception("Dados de resultado não disponíveis para a data solicitada.")

    return resultado_xml


@task(nout=3)
def transform(resultado_xml: str):
    soup = BeautifulSoup(resultado_xml, "xml")

    solicitacoes_rows = []
    exames_rows = []
    resultados_rows = []

    lote = soup.find("lote")

    for solicitacao in lote.find_all("solicitacao"):
        solicitacoes_row = solicitacao.attrs

        for entidade_suporte in ["responsaveltecnico", "paciente"]:
            for key, value in solicitacao.find(entidade_suporte).attrs.items():
                solicitacoes_row[f"{entidade_suporte}_{key}"] = value

        for key, value in lote.attrs.items():
            solicitacoes_row[f"lote_{key}"] = value

        solicitacao_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{solicitacoes_row.get('codigoLis', '')}|"
                f"{solicitacoes_row.get('codigoApoio', '')}|"
                f"{solicitacoes_row.get('dataPedido', '')}|"
                f"{solicitacoes_row.get('paciente_nome', '')}|"
                f"{solicitacoes_row.get('paciente_cpf', '')}",
            )
        )
        solicitacoes_row["id"] = solicitacao_id
        solicitacoes_rows.append(solicitacoes_row)

        for exame in solicitacao.find_all("exame"):
            exames_row = exame.attrs

            exame_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    f"{solicitacao_id}|"
                    f"{exames_row.get('codigoExame', '')}|"
                    f"{exames_row.get('codApoio', '')}|"
                    f"{exames_row.get('dataAssinatura', '')}",
                )
            )

            exames_row["id"] = exame_id
            exames_row["solicitacao_id"] = solicitacao_id

            for entidade_suporte in ["solicitante"]:
                for key, value in solicitacao.find(entidade_suporte).attrs.items():
                    exames_row[f"{entidade_suporte}_{key}"] = value

            exames_rows.append(exames_row)

            for resultado in exame.find_all("resultado"):
                resultado_row = resultado.attrs

                # Gerando ID determinístico para o resultado
                resultado_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_DNS,
                        f"{resultado_row.get('codigoApoio', '')}|"
                        f"{resultado_row.get('descricaoApoio', '')}|"
                        f"{exame_id}",
                    )
                )

                resultado_row["id"] = resultado_id
                resultado_row["exame_id"] = exame_id
                resultados_rows.append(resultado_row)

    # Convertendo para DataFrames
    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    solicitacoes_df = pd.DataFrame(solicitacoes_rows)
    exames_df = pd.DataFrame(exames_rows)
    resultados_df = pd.DataFrame(resultados_rows)

    for df in [solicitacoes_df, exames_df, resultados_df]:
        df["datalake_loaded_at"] = now

    return solicitacoes_df, exames_df, resultados_df


@task
def generate_extraction_windows(start_date: pd.Timestamp) -> List[Dict[str, str]]:
    """
    Gera janelas de extração de 3 horas por dia a partir de start_date até ontem.
    Retorna lista de dicionários com dt_inicio e dt_fim formatados (ex: YYYY-MM-DDTHH:MM:SS-0300)
    """

    tz = pytz.timezone("America/Sao_Paulo")

    if start_date.tzinfo is None:
        start_date = tz.localize(start_date)

    end_date = tz.localize(
        datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ) - timedelta(seconds=1)

    windows = []

    current_date = start_date
    while current_date <= end_date:
        for hour_ranges in [
            ("00:00:01", "03:00:00"),
            ("03:00:01", "06:00:00"),
            ("06:00:01", "09:00:00"),
            ("09:00:01", "12:00:00"),
            ("12:00:01", "15:00:00"),
            ("15:00:01", "18:59:59"),
            ("18:00:01", "21:59:59"),
            ("21:00:01", "23:59:59"),
        ]:
            start_time = f"{current_date.date()} {hour_ranges[0]}"
            end_time = f"{current_date.date()} {hour_ranges[1]}"

            dt_inicio, dt_fim = get_datetime_working_range.run(
                start_datetime=start_time,
                end_datetime=end_time,
                interval=1,
                return_as_str=True,
                timezone="America/Sao_Paulo",
            )

            windows.append(
                {
                    "dt_inicio": dt_inicio,
                    "dt_fim": dt_fim,
                }
            )

        current_date += timedelta(days=1)

    return windows


@task
def get_identificador_lis(cnes_lis: str) -> list:
    data = json.loads(cnes_lis)
    identificadores = [data[cnes] for cnes in cientificalab_constants.CNES.value]
    return identificadores


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
                    "identificador_lis": cnes,
                    "dt_inicio": window["dt_inicio"],
                    "dt_fim": window["dt_fim"],
                    "environment": env,
                }
            )
    return params
