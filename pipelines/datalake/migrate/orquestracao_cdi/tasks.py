# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

import io
import os
import re
from datetime import datetime
from typing import List, Optional

import pandas as pd
import pytz
import requests
from google.cloud import bigquery, storage
from prefect.engine.signals import FAIL

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_bigquery_project_from_environment
from pipelines.utils.time import parse_date_or_today

from .constants import constants
from .utils import format_tcm_case


# Para a justificativa quanto à existência dessa task,
# vide comentários no arquivo de flows
@task
def create_params_dict(environment: str = "prod", date: Optional[str] = None):
    return {"environment": environment, "date": date}


@task
def create_dbt_params_dict(environment: str = "prod"):
    # Queremos executar o seguinte comando:
    # $ dbt build --select +tag:cdi+ --target ENV
    return {
        "environment": environment,
        "rename_flow": True,
        "send_discord_report": False,
        "command": "build",
        "select": "+tag:cdi+",
        "exclude": None,
        "flag": None,
    }


@task
def create_tcm_params_dict(case_id: str, environment: str = "prod"):
    return {"environment": environment, "case_id": case_id}


@task
def fetch_tcm_cases(environment: str = "prod", date: Optional[str] = None) -> List[str]:
    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment=environment)

    DATASET = "intermediario_cdi"
    TABLE = "diario_rj_filtrado"
    DATE = parse_date_or_today(date).strftime("%Y-%m-%d")

    QUERY = f"""
SELECT voto
FROM `{project_name}.{DATASET}.{TABLE}`
WHERE voto is not NULL and data_publicacao = '{DATE}'
    """
    log(f"Querying for TCM cases...")
    rows = [str(row.values()[0]).strip() for row in client.query(QUERY).result()]
    log(f"Found {len(rows)} row(s); sample of 5: {rows[:5]}")

    return rows


@task
def get_todays_tcm_from_gcs(environment: str = "prod", skipped: bool = False):
    client = storage.Client()
    project_name = get_bigquery_project_from_environment.run(environment=environment)
    bucket = client.bucket(project_name)

    TODAY = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
    )

    YEAR_STR = TODAY.strftime("%Y")
    MONTH_STR = TODAY.strftime("%m")
    TODAY_STR = TODAY.strftime("%Y-%m-%d")
    if skipped:
        log(f"[!] Looking for TCM case files created ONLY on '{TODAY_STR}'!", level="warning")

    PATH = (
        f"staging/brutos_diario_oficial/processos_tcm/"
        f"ano_particao={YEAR_STR}/"
        f"mes_particao={MONTH_STR}/"
        f"data_particao={TODAY_STR}/"
    )
    log(f"Looking for CSV files in '{PATH}'")
    blobs = list(bucket.list_blobs(prefix=PATH, match_glob="**.csv"))
    log(f"Found {len(blobs)} CSV file(s) for TCM cases")
    log([blob.name for blob in blobs])

    # Arquivos são bem pequenos (<5KB) então vamos baixar direto em
    # memória; a princípio só vamos ter 1 por dia
    output_df = pd.DataFrame()
    for blob in blobs:
        log(f"Downloading '{blob.name}'...")
        data = blob.download_as_text()
        pseudofile = io.StringIO(data)
        df = pd.read_csv(pseudofile, dtype=str, encoding="utf-8")
        df1 = df[["processo_id", "decisao_data", "voto_conselheiro"]]
        log(f"'{blob.name}' has {len(df1)} row(s)")
        output_df = pd.concat([output_df, df1], ignore_index=True)

    # Se o dataframe possui a coluna (i.e. não está vazio)
    if "processo_id" in output_df.columns:
        # Padroniza números de processo do TCM
        output_df["processo_id"] = output_df["processo_id"].apply(format_tcm_case)

    log(f"Final TCM table has {len(output_df)} row(s)")
    return output_df


@task
def build_email(
    environment: str = "prod", date: Optional[str] = None, tcm_df: pd.DataFrame = None
) -> str:
    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment=environment)

    DATASET = "projeto_cdi"
    TABLE = "email"
    DO_DATETIME = parse_date_or_today(date)
    DATE = DO_DATETIME.strftime("%Y-%m-%d")

    QUERY = f"""
SELECT fonte, content_email, pasta, link, voto
FROM `{project_name}.{DATASET}.{TABLE}`
WHERE data_publicacao = '{DATE}'
    """
    log(f"Querying `{project_name}.{DATASET}.{TABLE}` for email contents for '{DATE}'...")
    rows = [row.values() for row in client.query(QUERY).result()]
    log(f"Found {len(rows)} row(s)")

    # Pegamos todos os processos do TCM relevantes
    tcm_case_numbers = []
    for _, _, _, _, voto in rows:
        voto = format_tcm_case(voto)
        if voto is None:
            continue
        tcm_case_numbers.append(voto)

    # Se tivemos algum processo relevante
    tcm_cases = dict()
    if len(tcm_case_numbers) > 0:
        log(f"Looking for TCM cases: {tcm_case_numbers}")
        if len(tcm_df) > 0:
            # Pega os votos, se tivermos essa informação
            relevant_tcm_df = tcm_df[tcm_df["processo_id"].isin(tcm_case_numbers)]
            relevant_tcm_df = relevant_tcm_df.reset_index()
            log(f"Found {len(relevant_tcm_df)} TCM case(s)")
            # Salva data e URL do voto, mapeado pelo ID
            for _, row in relevant_tcm_df.iterrows():
                # row => 'processo_id', 'decisao_data', 'voto_conselheiro'
                pid = row["processo_id"]
                tcm_cases[pid] = (row["decisao_data"], row["voto_conselheiro"])
        else:
            log(f"Empty TCM DataFrame")
    else:
        log(f"No TCM cases to get")

    def extract_header_from_path(path: str) -> str:
        if not path or len(path) <= 0:
            return None
        # Recebemos algo como
        # "AVISOS EDITAIS E TERMOS DE CONTRATOS/TRIBUNAL DE CONTAS DO MUNICÍPIO/OUTROS"
        # E queremos "Tribunal de Contas do Município"
        return re.sub(
            r"\bD([aeo])\b",
            r"d\1",
            (
                path.removeprefix("AVISOS EDITAIS E TERMOS DE CONTRATOS/")
                .removesuffix("/RESOLUÇÕES/RESOLUÇÃO N")
                .removesuffix("/DECRETOS N")
                .removesuffix("/OUTROS")
                .split("/")[-1]
                .title()
            ),
        )

    # Constrói cada bloco do email
    email_blocks = {}
    for row in rows:
        fonte, content_email, pasta, article_url, voto = row
        # Tentativa fútil de remover algumas entradas errôneas
        # Estamos tapando buracos no barco com chiclete aqui
        content = content_email.strip()
        # Chances basicamente nulas de XSS em email, mas isso
        # pode prevenir problemas de formatação acidental
        content = content.replace("<", "&lt;").replace(">", "&gt;")
        if content == "Anexo" or content.startswith(("•", "·")):
            log(f"`content` is invalid; skipping. Row: {row}", level="warning")
            continue
        # Somente grava conteúdo se não estiver vazio
        if len(content) <= 0:
            log(f"Empty `content`! Row: {row}", level="warning")
            continue

        if article_url is not None and len(article_url) > 0:
            content += f'<br/><a href="{article_url}">Abrir no D.O.</a>'

        voto = format_tcm_case(voto)
        if voto is not None and voto in tcm_cases:
            (vote_date, vote_url) = tcm_cases[voto]
            if vote_date and vote_url:
                content += f'<br/><a href="{vote_url}">Abrir voto no TCM</a> ({vote_date})'

        header = extract_header_from_path(pasta) or fonte
        if header not in email_blocks:
            email_blocks[header] = []
        email_blocks[header].append(content)

    # Confere primeiro se temos algum conteúdo para o email
    if not email_blocks:
        # Se não temos, retorna vazio
        return ""

    # [!] Importante: antes de editar o HTML abaixo, lembre que ele é HTML
    # para clientes de EMAIL. Assim, muitas (MUITAS) funcionalidades modernas
    # não estão disponíveis. Confira [https://www.caniemail.com/] para
    # garantir que suas modificações não quebrarão nada.
    formatted_date = DO_DATETIME.strftime("%d.%m.%Y")
    final_email_string = f"""
        <font face="sans-serif">
            <table style="max-width:650px;min-width:300px">
                <tr>
                    <td>
                        <img alt="Você Precisa Saber" width="650" style="width:100%"
                            src="https://storage.googleapis.com/sms_dit_arquivos_publicos/img/voce-precisa-saber-banner--1300px.png"/>
                    </td>
                </tr>
                <tr><td><hr/></td></tr>
                <tr>
                    <td style="padding:18px 0px">
                        <h2 style="margin:0;text-align:center">
                            <font color="#13335a" size="5">DESTAQUES &ndash; D.O. RIO de {formatted_date}</font>
                        </h2>
                    </td>
                </tr>
    """

    for header, body in email_blocks.items():
        # Se não há conteúdo sob esse cabeçalho (ex.: tudo foi filtrado)
        if len(body) <= 0:
            continue

        # Escreve cabeçalho, abre lista de conteúdos
        final_email_string += f"""
            <tr>
                <th style="background-color:#eceded;padding:9px">
                    <font color="#13335a">{header}</font>
                </th>
            </tr>
            <tr>
                <td style="padding:9px 18px">
                    <ul style="padding-left:9px">
        """
        for content in sorted(body, key=str.lower):
            content: str
            # Remove quebras de linha duplicadas, converte para <br>
            content = re.sub(r"\n{2,}", "\n", content.replace("\r", "")).replace("\n", "<br/>")
            content = re.sub(r"(<br/>){2,}", "<br/>", content)
            # Tentativa fútil de remover nomes em assinaturas que
            # às vezes aparecem em cabeçalhos
            filtered_content = re.sub(
                r"^((EDUARDO PAES|DANIEL SORANZ|ANEXO)\s*)+", "", content, flags=re.IGNORECASE
            )
            # Aqui potencialmente apagamos o conteúdo inteiro; então confere
            # primeiro antes de sobrescrever a variável final
            if len(filtered_content) > 0:
                content = filtered_content
            else:
                log(f"Filtering `content` empties it. Value: {content}", level="warning")

            # Negrito em decisões de TCM
            content = re.sub(
                r"^(.+)\s+nos\s+termos\s+do\s+voto\s+do\s+Relator",
                r"<b>\1</b> nos termos do voto do Relator",
                content,
            )
            # (nem todas terminam com "nos termos do voto do Relator")
            content = re.sub(
                r"^([^\<a-z][^a-z]+)\s+-\s+Processo\b",
                r"<b>\1</b> - Processo",
                content,
            )
            # Negrito em títulos de decretos/resoluções
            content = re.sub(
                r"^[\*\.]*((DECRETO|RESOLUÇÃO)\s+.+\s+DE\s+2[0-9]{3})\b",
                r"<b>\1</b>",
                content,
            )

            final_email_string += f"""
                <li style="margin-bottom:9px;color:#13335a">{content}</li>
            """
        # /for
        final_email_string += """
                    </ul>
                </td>
            </tr>
        """
        # Espaçamento entre seções
        final_email_string += '<tr><td style="padding:9px"></td></tr>'

    timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")
    final_email_string += f"""
                <tr><td><hr/></td></tr>
                <tr>
                    <td>
                        <img alt="DIT-SMS" width="100" align="right" style="margin-left:18px;margin-bottom:70px"
                            src="https://storage.googleapis.com/sms_dit_arquivos_publicos/img/dit-horizontal-colorido--300px.png"/>
                        <p style="font-size:13px;color:#888;margin:0">
                            Compilado institucional da <b>Coordenadoria de Demandas Institucionais</b> (CDI),
                            com apoio técnico da <b>Diretoria de Inovação e Tecnologia</b> (DIT),
                            gerado às {timestamp}.
                        </p>
                    </td>
                </tr>
            </table>
        </font>
    """
    return re.sub(r"\s{2,}\<", "<", final_email_string)


@task
def send_email(
    api_base_url: str,
    token: str,
    message: str,
    date: Optional[str] = None,
):
    DATE = parse_date_or_today(date).strftime("%d/%m/%Y")
    is_html = True

    # Caso não haja DO no dia, recebemos um conteúdo vazia
    if not message or len(message) <= 0:
        message = f"""
Nenhuma matéria relevante encontrada nos Diários Oficiais de hoje!

Email gerado às {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")}.
        """.strip()
        is_html = False

    request_headers = {"x-api-key": token}
    request_body = {
        "to_addresses": [
            "pedro.marques@dados.rio",
            "vitoria.leite@dados.rio",
            "natachapragana.sms@gmail.com",
        ],
        "cc_addresses": [
            "daniel.lira@dados.rio",
            "herian.cavalcante@dados.rio",
            "karen.pacheco@dados.rio",
            "matheus.avellar@dados.rio",
            "polianalucena.sms@gmail.com",
        ],
        "bcc_addresses": [],
        "subject": f"Você Precisa Saber ({DATE})",
        "body": message,
        "is_html_body": is_html,
    }

    if api_base_url.endswith("/"):
        api_base_url = api_base_url.rstrip("/?#")
    endpoint = api_base_url + constants.EMAIL_ENDPOINT.value

    response = requests.request("POST", endpoint, headers=request_headers, json=request_body)
    response.raise_for_status()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    response.encoding = response.apparent_encoding
    resp_json = response.json()
    if "success" in resp_json and resp_json["success"]:
        log("Email delivery requested successfully")
        return

    raise FAIL(f"Email delivery failed: {resp_json}")
