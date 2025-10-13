# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

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
def fetch_cids(environment: str, date: str) -> Tuple[list | str, bool]:
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
        return ([row for row in query_job.result()], False)
    except Exception as e:
        log(f"Error fetching from BigQuery: {repr(e)}", level="error")
        return (e, True)


@task(nout=2)
def build_email(cids: list | str, date: str | None, error: bool) -> Tuple[str, bool]:
    # Data de geração do email
    timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S de %d/%m/%Y")

    if error:
        log(f"Received error flag from previous task; returning error message", level="warning")
        return (
            f"""
            Erro ao obter dados do BigQuery: {repr(cids)}.
            Email gerado às {timestamp}.
            """,
            True,
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
    log(f"Got {len(cids)} total occurrence(s)")

    if len(cids) <= 0:
        log("No occurrences found with the specified CIDs", level="warning")
        return (
            f"""
            Nenhum atendimento encontrado com os CIDs solicitados para o dia {formatted_date}.
            Email gerado às {timestamp}.
            """,
            True,
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

        # `<a href="esse link enorme aí">...</a>` é muito byte, precisei tirar :\
        # url = f"https://cnes.datasus.gov.br/pages/estabelecimentos/consulta.jsp?search={cnes}"
        health_unit_str = f"""
        <small>{estabelecimento} (CNES {cnes})
        </small>
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
            if len(re.findall(r"[çãáàéõóíú]", cid_descricao, re.IGNORECASE) or []) > len(
                re.findall(r"[çãáàéõóíú]", descriptions[cid], re.IGNORECASE) or []
            ):
                descriptions[cid] = cid_descricao
        else:
            descriptions[cid] = cid_descricao

    # Nosso problema:
    # - Gmail tem um limite de ~100KB para uma mensagem antes de cortar ela
    #   (não queremos bater nesse limite);
    # - Poderíamos usar classes e <style> para diminuir significativamente o
    #   tamanho do HTML resultante. Porém, pelo que eu investiguei:
    #   - No cliente do Gmail, um <style> só funciona dentro de um <head>;
    #   - Ao receber um email, o cliente do Gmail:
    #     - Puxa o <style> do <head> do email para o <head> da página;
    #     - Remove o <head> do HTML do email.
    #   - Ao encaminhar o email recebido, por alguma idiotice cósmica, o cliente
    #     só encaminha o HTML do corpo, e esquece de mandar também o <style> que
    #     ele mesmo arrancou fora da mensagem.
    # - Ou seja, precisamos usar estilos inline, `style="..."`, mas com mais de
    #   100 atendimentos em um dia, precisamos ir a extremos pra não bater no
    #   limite de ~100KB (eba codegolfing):
    #   - Remover basicamente todo o whitespace possível;
    #   - Remover estilos que eu usaria em um mundo ideal, mas que não valem os bytes;
    #   - Usar <div> ao invés de <p style="margin:0">;
    #   - Converter cores pro mais próximo possível que caiba em 3 bytes
    #     (por exemplo, #788087 -> #778 = #777788; #f1f1f1 -> #eee);
    #   - <small> em tudo que precisar de textos pequenos, nada de font-size;
    #     - Podemos economizar +1B usando <font size=2 color="#778">...</font>
    #       ao invés do equivalente       <small style="color:#778">...</small>
    #   - "18px" (4B)? Você quis dizer "1em" (3B) = 16px;
    #   - "margin-bottom:1em" (17B) -> "margin:0 0 1em" (14B);
    #   - Fechar tags? Nesse mundo tão corrido, por que você faria isso

    # Começa construção do email
    email_string = f"""
<table style="font-family:sans-serif;max-width:41em;min-width:19em;color:#13335a">
    <tr>
        <td>
            <img width="100" align="right" src="{
                informes_seguranca_constants.LOGO_SMS_HORIZONTAL_COLORIDO.value
            }" alt="SMS"/>
            <h2 style="margin-top:0">Comunicação de CIDs – {formatted_date}
    </tr>
    """

    unique_cpfs = len(cpfs_with_occurrence)
    unique_cpf_string = unique_cpfs
    if unknown_patients > 0:
        s = "" if unknown_patients < 2 else "s"
        unique_cpf_string = (
            f"{unique_cpfs-1} <small>(+{unknown_patients} paciente{s} não identificado{s})</small>"
        )

    unique_cids = len(occurrences.keys())
    email_string += f"""
    <tr>
        <td>
            <div>
                <b>Total de CPFs:</b> {unique_cpf_string}<br>
                <b>Total de atendimentos:</b> {len(cids)}<br>
                <b>CIDs distintos:</b> {unique_cids}
    </tr>
    """

    group_range = None
    all_cids: List[str] = sorted(occurrences.keys())
    for i, cid in enumerate(all_cids):
        # Cabeçalho de grupo de CIDs
        (current_group_range, current_group_description) = utils.get_cid_group(cid)
        # Se temos uma nova categoria de CIDs
        if current_group_range != group_range:
            # Adiciona categoria
            email_string += f"""
            <tr>
                <th style="background:#fee;border-top:2px solid#e66;padding:4px 8px;text-align:left">
                    <small>{current_group_range}:</small>
                    {utils.filter_CID_group(current_group_description)}
            </tr>
            """
            # Atualiza categoria atual
            group_range = current_group_range

        # Se a próxima iteração é uma nova categoria de CIDs
        do_space_bottom = False
        if i + 1 < len(all_cids):
            next_cid = all_cids[i + 1]
            (next_group_range, _) = utils.get_cid_group(next_cid)
            if next_group_range != group_range:
                # Ativa flag para espaçamento no fim da lista
                do_space_bottom = True

        # Cabeçalho de CID
        email_string += f"""
        <tr>
            <th style="background:#eee;padding:4px 8px;text-align:left">
                <small style="background:#fff;border-radius:4px;padding:2px 4px">{cid}</small>
                {utils.filter_CID_group(descriptions[cid])}
        </tr>
        """

        # Número de ocorrências de cada CID
        # Devido às restrições agressivas de tamanho, só mostramos se count > 3
        cid_occurrences_count = len(occurrences[cid])
        if cid_occurrences_count > 3:
            email_string += f"<tr><td><small>{cid_occurrences_count} ocorrências</tr>"

        email_string += """
        <tr>
            <td style="padding:0 0 1em">
                <ul style="padding:0 2em;margin:0">
        """
        # Pacientes com CID especificado
        for j, patient in enumerate(occurrences[cid]):
            is_last_child = j == len(occurrences[cid]) - 1
            custom_margin = "margin:0"
            if j > 0:
                if not do_space_bottom or not is_last_child:
                    custom_margin = "margin:4px 0"
                else:
                    custom_margin = "margin:4px 0 1em"
            elif do_space_bottom and is_last_child:
                custom_margin = "margin:0 0 1em"

            has_exit_dt = bool(patient["exit"])
            id_is_uuid4 = bool(
                re.fullmatch(
                    r"[A-Z0-9]+\-[A-Z0-9]+\-[A-Z0-9]+\-[A-Z0-9]+\-[A-Z0-9]+", str(patient["id"])
                )
            )

            # TODO: ficou de fora por ora: patient["status"] (status do CID)
            email_string += f"""
            <li style="{custom_margin};padding:4px 8px;background:#fafafa">
                <div>{patient["name"]}{patient["age"]}</div>
                <div>{patient["health_unit"]}</div>
                <div>
                    <small style="padding:2px 4px;background:#eee{
                        ";margin-right:4px" if has_exit_dt or not id_is_uuid4 else ""
                    }">Entrada: {
                        patient["entry"]
                    }</small>
            """
            if patient["exit"]:
                email_string += f"""
                    <small style="padding:2px 4px;background:#eee{
                        ";margin-right:4px" if not id_is_uuid4 else ""
                    }">Saída: {
                        patient["exit"]
                    }</small>
                """

            # Para economizar espaço, vamos ignorar supostos IDs de prontuários Vitai, que
            # vêm como UUIDv4. Ex.: ABCDEF01-1234-4ABC-DEF0-ABCDEF012345
            if not id_is_uuid4:
                email_string += f"""<font size=1 color="#778">Prontuário [{patient["id"]}]</font>"""

            email_string += f"""
                </div>
            </li>
            """

        email_string += """
        </tr>
        """

    email_string += f"""
    <tr style="height:2em"><td><hr></tr>
    <tr>
        <td>
            <img width="100" align="right" src="{
                informes_seguranca_constants.LOGO_DIT_HORIZONTAL_COLORIDO.value
            }" alt="DIT-SMS" style="margin-left:1em;margin-bottom:4em"/>
            <div style="font-size:13px;color:#888">
              Apoio técnico da <b>Diretoria de Inovação e Tecnologia</b> (DIT),
              parte da <b>Secretaria Municipal de Saúde</b> (SMS).
              Email gerado às {timestamp}.
            </div>
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
        "subject": f"Comunicação de CIDs – {formatted_date}",
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
