# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from datetime import datetime

MONTH = [
    "jan",
    "fev",
    "mar",
    "abr",
    "maio",
    "jun",
    "jul",
    "ago",
    "set",
    "out",
    "nov",
    "dec",
]


def format_datetime(dt: datetime | None) -> str | None:
    if not dt:
        return None
    # Se recebemos 'T00:00:00', retorna somente "dd/mês"
    if dt.hour + dt.minute + dt.second == 0:
        return f"{dt.day:02}/{MONTH[dt.month-1]}"
    # Retorna data/hora no formato "dd/mês hh:mm"
    return f"{dt.day:02}/{MONTH[dt.month-1]} {dt.hour:02}:{dt.minute:02}"
