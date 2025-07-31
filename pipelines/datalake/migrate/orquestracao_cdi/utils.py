# -*- coding: utf-8 -*-
import re
from google.cloud import bigquery

from pipelines.utils.logger import log


def format_tcm_case(case_num: str) -> str | None:
    if case_num is None or not case_num:
        return None
    case_num = str(case_num).strip()
    if len(case_num) <= 0:
        return None

    # Exemplo: '040/100420/2019'
    case_regex = re.compile(r"(?P<sec>[0-9]+)/(?P<num>[0-9]+)/(?P<year>[0-9]{4})")
    m = case_regex.search(case_num)
    if m is None:
        raise ValueError(f"'{case_num}' is not a valid TCM case number ([0-9]+/[0-9]+/[0-9]{{4}})")
    # Padding para transformar "40" -> "040"
    sec = m.group("sec").rjust(3, "0")
    num = m.group("num")
    year = m.group("year")
    assert len(year) == 4, f"[{sec}/{num}/{year}] Year '{year}' has length {len(year)}; expected 4"
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
    DOU = True
    DORJ = True
    for (dotype, success) in rows:
        dotype: str
        success: str
        if success == "false":
            # Se qualquer seção do DOU falhou, cancela envio inteiro
            if dotype.startswith("dou"):
                DOU = False
            elif dotype.startswith("dorj"):
                DORJ = False

    output = {
        "dorj": DORJ,
        "dou": DOU
    }
    log(output)
    return output
