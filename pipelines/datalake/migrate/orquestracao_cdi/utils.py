# -*- coding: utf-8 -*-
import re

from google.cloud import bigquery

from pipelines.utils.logger import log


def format_tcm_case(case_num: str) -> str | None:
    if case_num is None or not case_num:
        return None
    case_num = str(case_num).strip()
    if len(case_num) <= 0 or case_num.lower() == "none" or case_num.lower() == "null":
        return None

    # Exemplo: '040/100420/2019'
    case_regex = re.compile(r"(?P<sec>[0-9]+)/(?P<num>[0-9]+)/(?P<year>[0-9]{4})")
    m = case_regex.search(case_num)
    if m is None:
        log(
            f"'{case_num}' is not a valid TCM case number ([0-9]+/[0-9]+/[0-9]{{4}})",
            level="warning",
        )
        return None
    # Padding para transformar "40" -> "040"
    sec = m.group("sec").rjust(3, "0")
    num = m.group("num")
    year = m.group("year")
    if len(year) != 4:
        log(
            f"[{sec}/{num}/{year}] Year '{year}' has length {len(year)}; expected 4",
            level="warning",
        )
    return f"{sec}/{num}/{year}"


def get_latest_extraction_status(project: str, do_date: str):
    client = bigquery.Client()

    DATASET = "projeto_cdi"
    TABLE = "extracao_status"
    FULL_TABLE = f"`{project}.{DATASET}.{TABLE}`"

    DATE = do_date

    # Retorna (tipo, status) da atualização mais recente da extração
    # para cada tipo de D.O., para o diário de uma determinada data
    QUERY = f"""
with sorted as (
  select
      *,
      row_number() over(
        partition by tipo_diario order by _updated_at desc
      ) as row_num
  from {FULL_TABLE}
  where data_publicacao = '{DATE}'
)
select tipo_diario, extracao_sucesso
from sorted
where row_num = 1
    """
    log(f"Querying {FULL_TABLE} for success statuses for '{DATE}'...")
    # Algo como:
    # | tipo_diario | extracao_sucesso
    # | ------------------------------
    # | dorj        | true
    # | dou-sec3    | false
    # | ...
    rows = [row.values() for row in client.query(QUERY).result()]
    log(f"Found {len(rows)} row(s)")

    # Presume que foi bem sucedido a não ser que encontre um 'false'
    success_status = {"dorj": None, "dou": None}
    for dotype, success in rows:
        dotype = str(dotype).lower().strip()
        success = str(success).lower().strip()
        do = None
        if dotype.startswith("dou"):
            do = "dou"
        elif dotype.startswith("dorj"):
            do = "dorj"
        if do is None:
            log(f"Got unrecognized tipo_diario='{dotype}'; skipping", level="warning")
            continue

        # Se qualquer seção do diário falhou, cancela envio inteiro dele
        if success == "false":
            success_status[do] = False
            continue
        # Senão, só marca como True se for o primeiro status
        elif success == "true":
            if success_status[do] is None:
                success_status[do] = True
            # Status subsequentes seriam um AND com true, mas isso é no-op
            # else: success_status[do] &= True
        else:
            log(f"Got unrecognized extracao_sucesso='{success}'; skipping", level="warning")
            continue

    log(success_status)
    no_status = [dotype for (dotype, status) in success_status.items() if status is None]
    if len(no_status) > 0:
        log(
            f"Unspecified extraction status for type(s): {no_status}; treating as failures",
            level="warning",
        )
        for do in no_status:
            success_status[do] = False
    return success_status
