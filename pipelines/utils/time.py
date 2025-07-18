# -*- coding: utf-8 -*-
import re
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import prefect
import pytz
from dateutil import parser

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def from_relative_date(relative_date: Optional[str] = None) -> Optional[pd.Timestamp]:
    if relative_date is None:
        log("Relative date is None, returning None", level="info")
        return None

    result = None
    if relative_date.startswith("D-"):
        days = int(relative_date.split("-")[1])
        result = pd.Timestamp.now().date() - pd.DateOffset(days=days)
    elif relative_date.startswith("M-"):
        months = int(relative_date.split("-")[1])
        result = pd.Timestamp.now().replace(day=1).date() - pd.DateOffset(months=months)
    elif relative_date.startswith("Y-"):
        years = int(relative_date.split("-")[1])
        result = pd.Timestamp.now().replace(day=1, month=1).date() - pd.DateOffset(years=years)
    else:
        log("The input dated is not a relative date, converting to datetime", level="info")
        result = pd.to_datetime(relative_date)

    log(f"Relative date is {relative_date}, returning {result}", level="info")
    return result


@task(nout=2)
def get_datetime_working_range(
    start_datetime: str = "",
    end_datetime: str = "",
    interval: int = 1,
    return_as_str: bool = False,
    timezone: str = None,
):
    logger = prefect.context.get("logger")

    logger.info("Calculating datetime range...")
    if start_datetime == "" and end_datetime == "":
        logger.info(
            f"No start/end provided. Using {interval} as day interval and scheduled date as end"
        )  # noqa
        scheduled_datetime = prefect.context.get("scheduled_start_time")
        end_datetime = scheduled_datetime.date()
        start_datetime = end_datetime - timedelta(days=interval)
    elif start_datetime == "" and end_datetime != "":
        logger.info("No start provided. Using end datetime and provided interval")
        end_datetime = pd.to_datetime(end_datetime)
        start_datetime = end_datetime - timedelta(days=interval)
    elif start_datetime != "" and end_datetime == "":
        logger.info("No end provided. Using start datetime and provided interval")
        start_datetime = pd.to_datetime(start_datetime)
        end_datetime = start_datetime + timedelta(days=interval)
    else:
        logger.info("Start and end datetime provided. Using them.")
        start_datetime = pd.to_datetime(start_datetime)
        end_datetime = pd.to_datetime(end_datetime)

    logger.info(f"Target date range: {start_datetime} -> {end_datetime}")

    if timezone is None:
        timezone = "America/Sao_Paulo"
    tz = pytz.timezone(timezone)

    if isinstance(start_datetime, date) and not isinstance(start_datetime, datetime):
        start_datetime = datetime.combine(start_datetime, datetime.min.time())

    if isinstance(end_datetime, date) and not isinstance(end_datetime, datetime):
        end_datetime = datetime.combine(end_datetime, datetime.min.time())

    if start_datetime.tzinfo is None:
        start_datetime = tz.localize(start_datetime)
    if end_datetime.tzinfo is None:
        end_datetime = tz.localize(end_datetime)

    start_datetime = start_datetime.astimezone(tz)
    end_datetime = end_datetime.astimezone(tz)

    if return_as_str:
        return start_datetime.isoformat(), end_datetime.isoformat()

    return start_datetime, end_datetime


@task
def get_dates_in_range(minimum_date: date | str, maximum_date: date | str) -> list[date]:
    """
    Returns a list of dates from the minimum date to the current date.

    Args:
        minimum_date (date): The minimum date.

    Returns:
        list: The list of dates.
    """
    if isinstance(maximum_date, str):
        maximum_date = date.fromisoformat(maximum_date)

    if minimum_date == "":
        return [maximum_date]

    if isinstance(minimum_date, str):
        minimum_date = date.fromisoformat(minimum_date)

    return [minimum_date + timedelta(days=i) for i in range((maximum_date - minimum_date).days)]


def parse_date_or_today(date: Optional[str]) -> datetime:
    """
    Recebe string de data (ex.: "2025-07-18", "30/10/1999")
    e retorna objeto `datetime` a partir dela. Se receber
    string vazia ou `None`, retorna `datetime.now`.

    Os formatos esperados são YYYY-MM-DD e DD/MM/YYYY. A
    função tenta fazer parsing de outros formatos via
    `parser.parse(date, ignoretz=True, dayfirst=True)` mas,
    se possível, use um dos formatos esperados.

    Exemplos de uso:
    ```
    >>> parse_date_or_today("")
    datetime.datetime(2025, 7, 18, 0, 0)
    >>> parse_date_or_today(None)
    datetime.datetime(2025, 7, 18, 0, 0)
    >>> parse_date_or_today("2012-01-05")
    datetime.datetime(2012, 1, 5, 0, 0)
    >>> parse_date_or_today("05/01/2012")
    datetime.datetime(2012, 1, 5, 0, 0)
    >>> parse_date_or_today("05-01-2012")
    datetime.datetime(2012, 1, 5, 0, 0)
    ```
    """
    # Se não recebeu data nenhuma
    if date is None or len(date) <= 0:
        # Retorna a data atual em formato igual ao retornado
        # pelas funções de parsing
        return datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

    # Caso contrário, vamos tentar interpretar a string recebida
    date = date.strip()
    # Se recebemos o formato ISO 'YYYY-MM-DD'
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
        return datetime.fromisoformat(date)
    # Se recebemos o formato 'DD/MM/YYYY'
    if re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", date):
        return datetime.strptime(date, "%d/%m/%Y")
    # Senão, faz última tentativa de parsing da string, pode dar erro
    return parser.parse(date, ignoretz=True, dayfirst=True)
