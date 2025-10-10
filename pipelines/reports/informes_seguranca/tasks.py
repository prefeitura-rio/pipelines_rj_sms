# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from datetime import datetime, timedelta
import re
from typing import Optional, Tuple

import pytz
import requests
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from prefect.engine.signals import FAIL

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.time import get_age_from_birthdate, parse_date_or_today

from . import utils as utils
from .constants import informes_seguranca_constants


@task(nout=2)
def fetch_cids(environment: str, date: str) -> Tuple[RowIterator | str, bool]:
    # Aqui temos duas opções para filtar em SQL pelos CIDs desejados:
    # - ~70 `starts_with()`s seguidos, em uma corrente de `or`;
    # - um regexp_contains recebendo `^(cid1|cid2|cid3|....)`.
    # Acho que em termos de custo ambos dão no mesmo, mas o RegEx é mais
    # limpinho arrumadinho então vamos com ele
    CIDs = informes_seguranca_constants.CIDS.value
    regex = f"^({'|'.join(CIDs)})"

    project = "rj-sms"
    dataset = "saude_historico_clinico"
    episode_table = "episodio_assistencial"
    patient_table = "paciente"

    # Se requisitou data específica, usa; senão, pega data de ontem
    requested_dt = parse_date_or_today(date, subtract_days_from_today=1)
    formatted_date = requested_dt.strftime("%Y-%m-%d")
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
        from `{project}.{dataset}.{episode_table}` ep,
            unnest(ep.condicoes) condicao
        left join `{project}.{dataset}.{patient_table}` pac
            on safe_cast(ep.paciente_cpf as INT64) = pac.cpf_particao
        where data_particao = "{formatted_date}"
            and regexp_contains(condicao.id, r'{regex}')
        qualify row_number() over (
            partition by entrada, cnes, cpf, cid
            order by saida desc
        ) = 1
        order by
            cid asc,
            coalesce(nome_social, nome) asc nulls last
    """
    log(f"Running query:\n{query}")

    try:
        client = bigquery.Client()
        query_job = client.query(query)
        return (query_job.result(), False)
    except Exception as e:
        log(f"Error fetching from BigQuery: {repr(e)}", level="error")
        return (e, True)


@task(nout=2)
def build_email(cids: RowIterator | str, date: str | None, error: bool) -> Tuple[str, bool]:
    # Data de geração do email
    timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")

    if error:
        log(f"Received error flag from previous task; returning error message", level="warning")
        return (
            f"""
            Erro ao obter dados do BigQuery: {repr(cids)}.
            Email gerado às {timestamp}.
            """,
            True
        )

    requested_dt = parse_date_or_today(date, subtract_days_from_today=1)
    formatted_date = requested_dt.strftime("%d/%m/%Y")
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
    log(f"Got {cids.total_rows} total occurrence(s)")

    if cids.total_rows <= 0:
        log("No occurrences found with the specified CIDs", level="warning")
        return (
            f"""
            Nenhum atendimento encontrado com os CIDs solicitados para o dia {formatted_date}.
            Email gerado às {timestamp}.
            """,
            True
        )

    def isoformat_if(dt: datetime | None) -> str | None:
        return dt.isoformat() if dt else None

    occurrences = dict()
    descriptions = dict()
    cpfs_with_occurrence = set()
    unknown_patients = 0
    for row in cids:
        record_id = row["id_prontuario"]

        dt_entrada = utils.format_datetime(row["entrada"])
        dt_saida = utils.format_datetime(row["saida"])

        cnes = row["cnes"]
        estabelecimento = (
            str(row["estabelecimento"] or "").strip() or "<i>Sem informação da unidade</i>"
        )
        health_unit_str = f"""
        <span style='font-size:70%'>
            <a href='https://cnes.datasus.gov.br/pages/estabelecimentos/consulta.jsp?search={cnes}'>{estabelecimento}</a>
            (CNES {cnes})
        </span>
        """

        cpf = row["cpf"]
        cpfs_with_occurrence.add(cpf)
        if cpf is None or not cpf:
            unknown_patients += 1

        nome = row["nome"]
        nome_social = row["nome_social"]
        name_str = utils.build_name_string(cpf, nome, nome_social)

        age = get_age_from_birthdate(isoformat_if(row["data_nascimento"]))
        cid = row["cid"]
        cid_descricao = row["cid_descricao"]
        cid_situacao = row["cid_situacao"]

        if cid not in occurrences:
            occurrences[cid] = []

        occurrence_obj = {
            "id": record_id,
            "name": name_str,
            "age": f", {age} ano{'' if age < 2 else 's'}" if age else "",
            "health_unit": health_unit_str,
            "entry": dt_entrada,
            "exit": dt_saida,
            "status": cid_situacao,
        }
        occurrences[cid].append(occurrence_obj)

        if cid in descriptions and cid_descricao != descriptions[cid]:
            log(
                f"Mismatched descriptions for CID '{cid}':\n\t'{descriptions[cid]}';\n\t'{cid_descricao}'",
                level="warning",
            )
            # Aqui temos zero possibilidade de saber qual o correto
            # Ao invés disso, tentamos pegar o mais 'completo'
            # Ex.: "forca" é pior que "força"
            if len(re.findall(r'[çãáàéõóíú]', cid_descricao, re.IGNORECASE) or []) \
                > len(re.findall(r'[çãáàéõóíú]', descriptions[cid], re.IGNORECASE) or []):
                descriptions[cid] = cid_descricao
        else:
            descriptions[cid] = cid_descricao

    # Começa construção do email
    email_string = f"""
<table style="font-family:sans-serif;max-width:650px;min-width:300px">
    <tr>
        <td style="padding:18px 0">
            <h2 style="margin:0;text-align:center;color:#13335a;font-size:24px">
                Informe de Segurança – {formatted_date}
            </h2>
        </td>
    </tr>
    """

    unique_cpfs = len(cpfs_with_occurrence)
    unique_cpf_string = unique_cpfs
    if unknown_patients > 0:
        s = "" if unknown_patients < 2 else "s"
        unique_cpf_string = f"{unique_cpfs-1} <span style='font-size:80%'>(+{unknown_patients} paciente{s} não identificado{s})</span>"

    unique_cids = len(occurrences.keys())
    email_string += f"""
    <tr>
        <td>
            <p style="margin:0;color:#788087;font-size:85%">Resumo:</p>
            <p style="margin:0;color:#13335a">
                <b>Total de CPFs:</b> {unique_cpf_string}<br>
                <b>Total de atendimentos:</b> {cids.total_rows}<br>
                <b>CIDs distintos:</b> {unique_cids}
            </p>
        </td>
    </tr>
    """

    group_range = None
    for cid in sorted(occurrences.keys()):
        # Cabeçalho de grupo de CIDs
        (current_group_range, current_group_description) = utils.get_cid_group(cid)
        # Se temos uma nova categoria de CIDs
        if current_group_range != group_range:
            # Adiciona categoria
            # OBS: Opções de cor aqui:
            # - Azul:     bg: #e6f1fe; borda: #007fff
            # - Vermelho: bg: #fee6e6; borda: #f16363
            # - Laranja marca da Prefeitura:
            #             bg: #ffeae5; borda: #f06949
            email_string += f"""
            <tr><td style="padding:9px"></td></tr>
            <tr>
                <th style="background-color:#fee6e6;border-top:2px solid #f16363;
                padding:4px 8px;color:#13335a;text-align:left">
                    <span style="font-size:85%">{current_group_range}:</span>
                    {utils.filter_CID_group(current_group_description)}
                </th>
            </tr>
            """
            # Atualiza categoria atual
            group_range = current_group_range

        # Cabeçalho de CID
        email_string += f"""
        <tr>
            <th style="background-color:#eff1f3;padding:4px 8px;color:#13335a;font-size:90%;text-align:left">
                <span style="font-size:80%;background-color:#fff;border-radius:4px;padding:2px 4px;margin-right:4px;display:inline-block">
                    {cid}
                </span>
                {descriptions[cid]}
            </th>
        </tr>
        """

        email_string += """
        <tr>
            <td style="padding:2px 0px 9px 18px">
                <ul style="padding-left:9px;margin:0">
        """
        # Pacientes com CID especificado
        for patient in occurrences[cid]:
            # TODO: ficou de fora por ora: patient["status"] (status do CID)
            email_string += f"""
            <li style="margin-bottom:4px;padding:4px 8px;background-color:#fafafa;color:#13335a">
                <p style="margin:0">{patient["name"]}{patient["age"]}</p>
                <p style="margin:0">{patient["health_unit"]}</p>
                <p style="margin:0;margin-top:2px">
                    <span style="font-size:80%;padding:2px 4px;background-color:#f1f1f1;border-radius:2px;margin-right:4px">Entrada: {
                        patient["entry"]
                    }</span>
            """
            if patient["exit"]:
                email_string += f"""
                    <span style="font-size:80%;padding:2px 4px;background-color:#f1f1f1;border-radius:2px;margin-right:4px">Saída: {
                        patient["exit"]
                    }</span>
                """

            email_string += f"""
                    <span style="font-size:70%;color:#788087">Prontuário [{patient["id"]}]</span>
                </p>
            </li>
            """
        email_string += """
                </ul>
            </td>
        </tr>
        """

    email_string += f"""
    <tr><td style="padding:9px"></td></tr>
    <tr><td><hr></td></tr>
    <tr>
        <td>
            <img style="margin-left:18px;margin-bottom:70px" width="100" align="right" src="{
                informes_seguranca_constants.LOGO_DIT_HORIZONTAL_COLORIDO.value
            }" alt="DIT-SMS"/>
            <p style="font-size:13px;color:#888;margin:0">
              Apoio técnico da <b>Diretoria de Inovação e Tecnologia</b> (DIT),
              parte da <b>Secretaria Municipal de Saúde</b> (SMS).
              Email gerado às {timestamp}.
            </p>
        </td>
    </tr>
</table>
    """

    email_string = utils.compress_message_whitespace(email_string)
    # FIXME
    with open("abc.html", "w") as f:
        f.write(email_string)

    return (email_string, False)


@task
def send_email(
    api_base_url: str,
    token: str,
    message: str,
    recipients: dict,
    error: bool,
    date: Optional[str] = None,
):
    requested_dt = parse_date_or_today(date, subtract_days_from_today=1)
    formatted_date = requested_dt.strftime("%d/%m/%Y")

    request_headers = {"x-api-key": token}
    request_body = {
        **recipients,
        "subject": f"Informes de Segurança – {formatted_date}",
        "body": message,
        "is_html_body": not error,
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
