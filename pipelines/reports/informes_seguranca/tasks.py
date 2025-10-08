# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

import pandas as pd
import pytz
import requests

from datetime import datetime
from typing import Optional
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.time import get_age_from_birthdate, parse_date_or_today

from prefect.engine.signals import FAIL

from .constants import informes_seguranca_constants
from . import utils as utils


@task
def fetch_cids(environment: str, date: str):
    # Ou pega a data recebida, ou pega a data de ontem
    date = parse_date_or_today(date, subtract_days_from_today=1)

    # Aqui temos duas opções para filtar em SQL pelos CIDs desejados:
    # - ~70 `starts_with()`s seguidos, em uma corrente de `or`;
    # - um regexp_contains recebendo `^(cid1|cid2|cid3|....)`.
    # Acho que em termos de custo ambos dao no mesmo, mas o RegEx é mais
    # limpinho arrumadinho então vamos com ele
    CIDs = informes_seguranca_constants.CIDS.value
    regex = f"^({'|'.join(CIDs)})"

    query = f"""
        select distinct
            ep.prontuario.id_prontuario_global as id_prontuario,
            entrada_datahora as entrada,
            saida_datahora as saida,
            estabelecimento.id_cnes as cnes,
            estabelecimento.nome as estabelecimento,
            case
                when paciente_cpf is null or paciente_cpf in ("", "None")
                then null
                else paciente_cpf
            end as cpf,
            pac.dados.nome as nome,
            pac.dados.nome_social as nome_social,
            paciente.data_nascimento as data_nascimento,
            condicao.id as cid,
            condicao.descricao as cid_descricao,
            condicao.situacao as cid_situacao
        from `rj-sms.saude_historico_clinico.episodio_assistencial` ep,
            unnest(ep.condicoes) condicao
        left join `rj-sms.saude_historico_clinico.paciente` pac
            on safe_cast(ep.paciente_cpf as INT64) = pac.cpf_particao
        where data_particao = "2025-10-06"
            and regexp_contains(condicao.id, r'{regex}')
        qualify row_number() over (
            partition by entrada, cnes, cpf, cid
            order by saida desc
        ) = 1
        order by cid asc, coalesce(nome_social, nome) asc
    """
    log(f"Running query:\n{query}")

    try:
        client = bigquery.Client()
        query_job = client.query(query)
        return query_job.result()
    except Exception as e:
        log(f"Error fetching from BigQuery: {repr(e)}", level="error")
        raise e


@task
def build_email(cids: RowIterator):
    # Aqui temos as seguintes colunas:
    # - id_prontuario -- ID ou no formato "<CNES>.<número>", ou UUID4
    # - entrada, saida -- data/hora no formato 'YYYY-MM-DDThh:mm:ss'; saída pode ser null
    # - cnes, estabelecimento -- identificadores da unidade de saúde, não modificamos
    # - cpf -- pode ser null
    # - nome, nome_social -- podem ser null
    # - data_nascimento -- formato YYYY-MM-DD, pode ser null
    # - cid -- formato [A-Z][0-9]{2,3}
    # - cid_descricao -- descrição em português do CID específico
    # - cid_situacao -- status do CID reportado; pode ser "ATIVO", "RESOLVIDO" ou "NAO ESPECIFICADO"
    log(cids)
    def isoformat_if(dt: datetime | None) -> str | None:
        return dt.isoformat() if dt else None

    for row in cids:
        dt_entrada = utils.format_datetime(row["entrada"])
        dt_saida = utils.format_datetime(row["saida"])

        cnes = row["cnes"]
        estabelecimento = row["estabelecimento"]
        cpf = row["cpf"]
        nome = (
            f"{row['nome_social']} ({row['nome']})"
            if row['nome_social']
            else row['nome']
        )
        age = get_age_from_birthdate(isoformat_if(row["data_nascimento"]))
        cid = row["cid"]
        cid_descricao = row["cid_descricao"]
        cid_situacao = row["cid_situacao"]

        log(f"{estabelecimento} ({cnes}) [{dt_entrada} | {dt_saida}] {nome} ({age} ano(s); CPF {cpf}): {cid} ({cid_situacao})")

    email_string = f"""
<table style="font-family:sans-serif;max-width:650px;min-width:300px">
    <tr>
        <td style="padding:18px 0">
            <h2 style="margin:0;text-align:center">
                <font color="#13335a" size="5">Informe de Segurança 08/10/2025</font>
            </h2>
        </td>
    </tr>
    """

    email_string += f"""
    <tr>
        <th style="background-color:#e6f1fe;padding:9px;color:#13335a;text-align:left">
            <span style="font-size:85%">X60–X84:</span>
            Lesões autoprovocadas intencionalmente
        </th>
    </tr>
    """

    email_string += f"""
    <tr>
        <th style="background-color:#eff1f3;padding:9px;color:#13335a;font-size:90%;text-align:left">
            <span style="font-size:80%;background-color:#fff;border-radius:4px;padding:2px 4px;margin-right:4px;display:inline-block">X649</span>
            Auto-intoxicação por e exposição, intencional, a outras drogas, medicamentos e substâncias biológicas e às não especificadas - local não especificado
        </th>
    </tr>
    """

    email_string += f"""
    <tr>
        <td style="padding:9px 18px">
            <ul style="padding-left:9px">
                <li style="margin-bottom:9px;color:#13335a">
                  <p style="margin:0">
                    Franciso José Maria Silva (37 anos), CPF 705.852.431-90
                  </p>
                  <p style="margin:0">
                    Hospital Municipal Salgado Filho <span style="font-size:70%">(CNES 2296306)</span>
                  </p>
                  <p style="margin:0">
                    <span style="font-size:80%;padding:2px 4px;background-color:#f1f1f1;border-radius:2px">Entrada: 06/out 20:22:15</span>
                    <span style="font-size:80%;padding:2px 4px;background-color:#f1f1f1;border-radius:2px">Saída: 06/out 23:24:15</span>
                  </p>
                </li>
                <li style="margin-bottom:9px;color:#13335a">
                  <p style="margin:0">
                    Joana Fernandes Carvalho (23 anos), CPF 987.654.321-00
                  </p>
                  <p style="margin:0">
                    CF Medalhista Olímpico Ricardo Lucarelli Souza <span style="font-size:70%">(CNES 9080163)</span>
                  </p>
                  <p style="margin:0">
                    <span style="font-size:80%;padding:2px 4px;background-color:#f1f1f1;border-radius:2px">Entrada: 06/out</span>
                  </p>
                </li>
            </ul>
        </td>
    </tr>
    """

    email_string += "<tr><td><hr></td></tr>"

    timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")
    email_string += f"""
    <tr><td style="padding:9px"></td></tr>
    <tr><td><hr></td></tr>
    <tr>
        <td>
            <img style="margin-left:18px;margin-bottom:70px" width="100" align="right" src="dit-horizontal-colorido--300px.png" alt="DIT-SMS"/>
            <p style="font-size:13px;color:#888;margin:0">
              Apoio técnico da <b>Diretoria de Inovação e Tecnologia</b> (DIT),
              parte da <b>Secretaria Municipal de Saúde</b> (SMS).
              Email gerado às {timestamp}.
            </p>
        </td>
    </tr>
</table>
    """
    return email_string


@task
def get_email_recipients(recipients: Optional[list | str] = None):
    # Se queremos sobrescrever os recipientes do email
    # (ex. enviar somente para uma pessoa, para teste)
    if recipients is not None:
        if type(recipients) is str:
            recipients = [recipients]
        if type(recipients) is list or type(recipients) is tuple:
            recipients = list(recipients)
            log(f"Overriding recipients ({len(recipients)}): {recipients}")
            return {
                "to_addresses": recipients,
                "cc_addresses": [],
                "bcc_addresses": [],
            }
        log(f"Unrecognized type for `recipients`: '{type(recipients)}'; ignoring")

    return {
        "to_addresses": [ "matheus.avellar@dados.rio" ],
        "cc_addresses": [],
        "bcc_addresses": [],
    }


@task
def send_email(
    api_base_url: str,
    token: str,
    message: str,
    recipients: dict,
    date: Optional[str] = None,
):
    DO_DATETIME = parse_date_or_today(date)
    DATE = DO_DATETIME.strftime("%d/%m/%Y")

    request_headers = {"x-api-key": token}
    request_body = {
        **recipients,
        "subject": f"Informes de Segurança - {DATE}",
        "body": message,
        "is_html_body": True,
    }

    if api_base_url.endswith("/"):
        api_base_url = api_base_url.rstrip("/?#")
    endpoint = api_base_url + informes_seguranca_constants.EMAIL_ENDPOINT.value

    response = requests.request("POST", endpoint, headers=request_headers, json=request_body)
    response.raise_for_status()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    response.encoding = response.apparent_encoding
    resp_json = response.json()
    if "success" in resp_json and resp_json["success"]:
        log("Email delivery requested successfully")
        return

    raise FAIL(f"Email delivery failed: {resp_json}")
