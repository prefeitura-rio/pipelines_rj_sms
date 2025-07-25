# -*- coding: utf-8 -*-
import re


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
