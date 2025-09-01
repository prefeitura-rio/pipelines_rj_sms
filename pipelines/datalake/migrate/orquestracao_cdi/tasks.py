# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

import io
import re
from datetime import datetime, timedelta
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
from .utils import (
    format_relevant_entry,
    format_tcm_case,
    get_current_edition,
    get_current_year,
    get_latest_extraction_status,
)


# Para a justificativa quanto à existência dessa task,
# vide comentários no arquivo de flows
@task
def create_dorj_params_dict(environment: str = "prod", date: Optional[str] = None):
    return {"environment": environment, "date": date}


@task
def create_dou_params_dict(environment: str = "prod", date: Optional[str] = None):
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
    PROJECT = get_bigquery_project_from_environment.run(environment=environment)
    DATASET = "projeto_cdi"
    TABLE = "email"
    FULL_TABLE = f"`{PROJECT}.{DATASET}.{TABLE}`"

    DO_DATETIME = parse_date_or_today(date)
    DATE = DO_DATETIME.strftime("%Y-%m-%d")

    CURRENT_YEAR = get_current_year()
    CURRENT_VPS_EDITION = get_current_edition(
        current_date=DO_DATETIME.replace(tzinfo=pytz.timezone("America/Sao_Paulo"))
    )

    QUERY = f"""
SELECT fonte, content_email, pasta, link, voto
FROM {FULL_TABLE}
WHERE data_publicacao = '{DATE}'
    """
    log(f"Querying {FULL_TABLE} for email contents for '{DATE}'...")
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

    # Pega status da última extração de cada
    extraction_status = get_latest_extraction_status(PROJECT, DATE)

    def extract_header_from_path(path: Optional[str]) -> str | None:
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

    def strip_if_not_none(v, default=None):
        stripped = str(v).strip()
        return stripped if v is not None and len(stripped) > 0 else default

    # Constrói cada bloco do email
    email_blocks: dict[str, List] = {}
    for row in rows:
        fonte, content, pasta, article_url, voto = row
        fonte = strip_if_not_none(fonte, default="Não categorizado")
        content = strip_if_not_none(content, default="")
        pasta = strip_if_not_none(pasta)
        article_url = strip_if_not_none(article_url, default="")
        tcm_case_id = strip_if_not_none(voto)

        # Pula diários se a extração não foi bem sucedida
        # ex. falhou no meio, etc
        if fonte.startswith("Diário Oficial da União") and not extraction_status["dou"]:
            continue
        if fonte.startswith("Diário Oficial do Município") and not extraction_status["dorj"]:
            continue

        # Chances basicamente nulas de XSS em email, mas isso
        # pode prevenir problemas de formatação acidental
        content = content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        # Tentativa fútil de remover algumas entradas errôneas;
        # estamos tapando buracos no barco com chiclete aqui
        content = content.replace("[tabela]", "").strip()
        if content == "Anexo" or content.startswith(("•", "·")):
            log(f"`content` is invalid; skipping. Row: {row}", level="warning")
            continue
        # Somente grava conteúdo se não estiver vazio
        if len(content) <= 0:
            log(f"Empty `content`! Row: {row}", level="warning")
            continue

        # Demanda: só adicionar link do DO se não tivermos voto do TCM
        do_link = f'<br/><a href="{article_url}">Abrir no D.O.</a>' if len(article_url) > 0 else ""
        tcm_case_id = format_tcm_case(tcm_case_id)
        # Se existe voto
        if tcm_case_id is not None:
            # Se temos o voto
            if tcm_case_id in tcm_cases:
                (vote_date, vote_url) = tcm_cases[tcm_case_id]
                # Se voto não está vazio
                if vote_url:
                    # Se temos todas as informações
                    if vote_date:
                        content += f'<br/><a href="{vote_url}">Abrir voto no TCM</a> ({vote_date})'
                    # Se temos URL, mas não data (não sei se é possível, mas não custa testar)
                    else:
                        log(
                            f"Vote missing date: '{tcm_case_id}' (url: '{vote_url}')",
                            level="warning",
                        )
                        content += f'<br/><a href="{vote_url}">Abrir voto no TCM</a>'
                # Voto existe e temos alguma informação, mas não o URL
                else:
                    log(
                        f"Not enough information for vote: vote_date: '{vote_date}', vote_url: '{vote_url}'",
                        level="warning",
                    )
                    content += f"{do_link}<br/><small>Não foi possível obter o voto no TCM</small>"
            # Voto existe, mas não temos nada sobre
            else:
                # Adiciona link do DO e justificativa para falta de voto
                log(f"Valid TCM case but we don't have it: '{tcm_case_id}'", level="warning")
                content += f"{do_link}<br/><small>Não foi possível obter o voto no TCM</small>"
        # Não existe voto
        else:
            # Adiciona somente link do DO
            content += do_link

        header = extract_header_from_path(pasta) or fonte
        if header not in email_blocks:
            email_blocks[header] = []
        email_blocks[header].append(content)

    ERRO_DOU = not extraction_status["dou"]
    ERRO_DORJ = not extraction_status["dorj"]
    ERRO_AMBOS = ERRO_DOU and ERRO_DORJ
    # Confere primeiro se temos algum conteúdo para o email
    if not email_blocks or ERRO_AMBOS:
        # Confere se a falta de conteúdo foi por falha na extração
        # TODO: pensar em forma mais elegante de fazer isso aqui; fiz meio corrido :x
        if ERRO_DOU or ERRO_DORJ:
            error_at = (
                "os Diários Oficiais (União e Município)"
                if ERRO_AMBOS
                else ("o Diário Oficial da União" if ERRO_DOU else "o Diário Oficial do Município")
            )
            success_at = ""
            if not ERRO_AMBOS:
                success_at = f"""
                    <p>
                        A extração do {
                            'Diário Oficial da União'
                            if not ERRO_DOU
                            else 'Diário Oficial do Município'
                        }, por sua vez, ocorreu normalmente,
                        mas não foram localizadas publicações
                        de interesse para a SMS-RJ.
                    </p>
                """
            return f"""
<font face="sans-serif">
    <p style="font-size:12px;color:#888;margin:0">
        <span style="margin-right:2px">Edição nº{CURRENT_VPS_EDITION}</span>
        &middot;
        <span style="margin-left:2px">Ano {CURRENT_YEAR}</span>
    </p>
    <p>
        <b>Atenção!</b>
        Não foi possível extrair automaticamente {error_at} de hoje.
        É possível que o website estivesse fora do ar no momento da extração.
        Por favor, confira manualmente.
    </p>{success_at}
    <p>
        Email gerado às
        {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")}.
    </p>
</font>
            """
        # Caso contrário, só não temos artigos relevantes hoje; retorna vazio
        return ""

    error_message = ""
    if ERRO_DOU or ERRO_DORJ:
        # Se estamos aqui, existe conteúdo relevante, mas um dos diários teve erro
        # na extração. Então montamos um aviso bonitinho em HTML pra mostrar
        # após os resultados relevantes
        error_at = "Diário Oficial da União" if ERRO_DOU else "Diário Oficial do Município"
        error_message = f"""
            <tr>
                <td style="background-color:#ecf5f9;color:#13335a;padding:9px 14px;border-radius:5px;border-left:5px solid #13335a">
                    <b>Atenção!</b> Não foi possível extrair automaticamente o {error_at} de hoje.
                    É possível que o website estivesse fora do ar no momento da extração.
                    Por favor, confira manualmente.
                </td>
            </tr>
            <tr><td style="padding:9px"></td></tr>
        """

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
                            src="{constants.BANNER_VPS.value}"/>
                    </td>
                </tr>
                <tr>
                    <td>
                        <p style="font-size:12px;color:#888;margin:0">
                            <span style="margin-right:6px">Edição nº{CURRENT_VPS_EDITION}</span>
                            &middot;
                            <span style="margin-left:6px">Ano {CURRENT_YEAR}</span>
                        </p>
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

        s = "" if len(body) < 2 else "s"
        # Escreve cabeçalho, abre lista de conteúdos
        final_email_string += f"""
            <tr>
                <th style="background-color:#eceded;padding:9px">
                    <font color="#13335a">{header}</font>
                </th>
            </tr>
            <tr>
                <td>
                    <small>{len(body)} artigo{s} relevante{s} encontrado{s}:</small>
                </td>
            </tr>
            <tr>
                <td style="padding:9px 18px">
                    <ul style="padding-left:9px">
        """
        for content in sorted(body, key=str.lower):
            filtered_content = format_relevant_entry(content)
            final_email_string += f"""
                <li style="margin-bottom:9px;color:#13335a">{filtered_content}</li>
            """
        # /for content in body
        final_email_string += """
                    </ul>
                </td>
            </tr>
        """
        # Espaçamento entre seções
        final_email_string += '<tr><td style="padding:9px"></td></tr>'
    # /for body in blocks

    # Adiciona mensagem de erro, se houver
    final_email_string += error_message

    # Rodapé
    timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")
    final_email_string += f"""
                <tr><td><hr/></td></tr>
                <tr>
                    <td>
                        <p style="color:#13335a;margin:0">
                            Coordenadoria de Demandas Institucionais<br/>
                            <b>S/SUBG/CDI/Gerência de Atendimento a Demandas de Controle Interno e Externo</b>
                        </p>
                    </td>
                </tr>
                <tr><td><hr></td></tr>
                <tr>
                    <td>
                        <img alt="DIT-SMS" width="100" align="right" style="margin-left:18px;margin-bottom:70px"
                            src="{constants.LOGO_DIT_HORIZONTAL_COLORIDO.value}"/>
                        <p style="font-size:13px;color:#888;margin:0">
                            Apoio técnico da <b>Diretoria de Inovação e Tecnologia</b> (DIT).<br/>
                            Email gerado às {timestamp}.
                        </p>
                    </td>
                </tr>
            </table>
        </font>
    """
    return re.sub(r"\s{2,}\<", "<", final_email_string)


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def get_email_recipients(environment: str = "prod", recipients: list = None) -> dict:
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

    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment=environment)

    DATASET = "brutos_sheets"
    TABLE = "cdi_destinatarios"

    QUERY = f"""
SELECT email, tipo
FROM `{project_name}.{DATASET}.{TABLE}`
    """
    log(f"Querying `{project_name}.{DATASET}.{TABLE}` for email recipients...")
    rows = [row.values() for row in client.query(QUERY).result()]
    log(f"Found {len(rows)} row(s)")

    to_addresses = []
    cc_addresses = []
    bcc_addresses = []
    for email, kind in rows:
        email = str(email).strip()
        kind = str(kind).lower().strip()
        if not email:
            continue
        if "@" not in email:
            log(f"Recipient '{email}' does not contain '@'; skipping", level="warning")
            continue

        if kind == "to":
            to_addresses.append(email)
        elif kind == "cc":
            cc_addresses.append(email)
        elif kind == "bcc":
            bcc_addresses.append(email)
        elif kind == "skip":
            continue
        else:
            log(
                f"Recipient type '{kind}' (for '{email}') not recognized; skipping", level="warning"
            )

    log(
        f"Recipients: {len(to_addresses)} (TO); {len(cc_addresses)} (CC); {len(bcc_addresses)} (BCC)"
    )
    return {
        "to_addresses": to_addresses,
        "cc_addresses": cc_addresses,
        "bcc_addresses": bcc_addresses,
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
    CURRENT_YEAR = get_current_year()
    CURRENT_VPS_EDITION = get_current_edition(
        current_date=DO_DATETIME.replace(tzinfo=pytz.timezone("America/Sao_Paulo"))
    )

    is_html = True

    # Caso não haja DO no dia, recebemos um conteúdo vazia
    if not message or len(message) <= 0:
        message = f"""
Edição nº{CURRENT_VPS_EDITION} · Ano {CURRENT_YEAR}

Nos Diários Oficiais de hoje, não foram localizadas publicações de interesse para a SMS-RJ.

Email gerado às {datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")}.
        """.strip()
        is_html = False

    request_headers = {"x-api-key": token}
    request_body = {
        **recipients,
        "subject": f"Você Precisa Saber - Edição {CURRENT_VPS_EDITION}ª - {DATE}",
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
