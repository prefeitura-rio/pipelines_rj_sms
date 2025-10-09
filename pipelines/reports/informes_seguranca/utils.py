# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

import re
from datetime import datetime
from typing import Tuple

from pipelines.utils.logger import log

from .constants import informes_seguranca_constants

MONTH = [
    "jan",
    "fev",
    "mar",  #
    "abr",
    "maio",
    "jun",  #
    "jul",
    "ago",
    "set",  #
    "out",
    "nov",
    "dec",  #
]


def format_datetime(dt: datetime | None) -> str | None:
    if not dt:
        return None
    # Se recebemos 'T00:00:00', retorna somente "dd/mês"
    if dt.hour + dt.minute + dt.second == 0:
        return f"{dt.day:02}/{MONTH[dt.month-1]}"
    # Retorna data/hora no formato "dd/mês hh:mm"
    return f"{dt.day:02}/{MONTH[dt.month-1]} {dt.hour:02}:{dt.minute:02}"


def format_cpf(cpf: str) -> str:
    if cpf is None or len(cpf) != 11:
        log(f"Expected CPF of length 11; got '{cpf}'", level="warning")
        return cpf
    # Retorna CPF no formato xxx.xxx.xxx-xx
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"


def build_name_string(cpf: str | None, name: str | None, social_name: str | None) -> str:
    remove_empty_str = lambda x: None if str(x or "").strip() == "" else x
    cpf = remove_empty_str(cpf)
    name = remove_empty_str(name)
    social_name = remove_empty_str(social_name)

    if cpf is None and name is None and social_name is None:
        return "<i>Paciente não identificado</i>"

    # Obrigatoriamente temos CPF aqui, pois nome/nome social vêm de CPF
    # Então formata ele logo
    cpf = format_cpf(cpf)

    # Se não temos nenhum nome, então retorna só o CPF
    if name is None and social_name is None:
        return f"CPF {cpf}"

    # Se não temos somente nome social, retorna "nome (cpf)"
    if social_name is None:
        return f"{name} <span style='font-size:70%'>(CPF {cpf})</span>"

    return f"{social_name} <span style='font-size:70%'>({name}; CPF {cpf})</span>"


def get_cid_group(cid: str) -> Tuple[str, str]:
    groups = informes_seguranca_constants.CID_groups.value

    for start, end, description in groups:
        # Nosso fim é inclusivo; por exemplo, Y891 é parte do grupo Y85-Y89
        # Porém, Y891 > Y89, então falha na comparação
        # Adicionando \uffff, não existe caso plausível em que Y89[x] > Y89[\uffff]
        # Seria o equivalente de adicionar \u0000 no início, mas não é necessário
        # porque já procede que Y85 < Y850
        if cid >= start and cid <= f"{end}\uffff":
            return (f"{start}–{end}", description)
    return ("?", "Grupo desconhecido")


def compress_message_whitespace(message: str) -> str:
    return re.sub(r"\s{2,}", " ", message)
