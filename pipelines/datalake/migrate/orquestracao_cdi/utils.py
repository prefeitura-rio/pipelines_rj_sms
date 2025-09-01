# -*- coding: utf-8 -*-
import re
from datetime import datetime

import pytz
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


def format_relevant_entry(content: str):
    # Remove quebras de linha duplicadas
    content = re.sub(r"\n{2,}", "\n", content.replace("\r", "")).strip()
    # Negrito em decisões de TCM
    content = re.sub(
        r"^([^\n\r]+)\s+nos\s+termos\s+do\s+voto\s+do\s+Relator",
        r"<b>\1</b> nos termos do voto do Relator",
        content,
    )
    # (nem todas terminam com "nos termos do voto do Relator")
    # Aqui usamos [^\S\n\r]:
    # - \S = NOT whitespace
    # - [^\S] = NOT (NOT whitespace) = whitespace
    # - [^\S\n\r] = whitespace, except \n, \r
    content = re.sub(
        r"^([^\<a-z\n\r][^a-z\n\r]+[^a-z\s\-])[^\S\n\r]*-?[^\S\n\r]*Processo\b",
        r"<b>\1</b> - Processo",
        content,
    )
    # Negrito em títulos de decretos/resoluções/atas
    content = re.sub(
        r"^[\*\.]*((DECRETO|RESOLUÇÃO|ATA|PORTARIA)\b[^\n\r]+\bDE\s+2[0-9]{3})\b",
        r"<b>\1</b>",
        content,
        flags=re.IGNORECASE,
    )

    # Finalmente, substitui quebras de linha por <br/>
    content = re.sub(r"(<br[ ]*/>){2,}", "<br/>", content.replace("\n", "<br/>"))
    return content


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


def get_current_year(current_date: datetime = None):
    date = current_date or datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    return date.year


def get_current_edition(current_date: datetime = None):
    # Edição do dia 28/08 foi 302
    first_edition = 302
    first_date = datetime(2025, 8, 28, tzinfo=pytz.timezone("America/Sao_Paulo"))
    # Queremos saber a edição dessa data
    last_date = current_date or datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    # Calculamos dias entre
    diff_days = (last_date - first_date).days

    # [Ref] https://stackoverflow.com/a/24494462/4824627
    since_weekday = first_date.isoweekday() + 1
    return first_edition + len(
        [x for x in range(since_weekday, since_weekday + diff_days) if x % 7 not in [0, 6]]
    )
