# -*- coding: utf-8 -*-
from typing import Optional

import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def to_list(element):
    return [element]


@task()
def as_dict(**kwargs):
    return kwargs


@task()
def as_datetime(date_str, format="%Y-%m-%d"):
    if not date_str or date_str == "":
        return None
    elif date_str == "now":
        return pd.Timestamp.now()
    else:
        return pd.to_datetime(date_str, format=format)


@task()
def from_relative_date(relative_date: Optional[str] = None):
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


@task()
def is_null_or_empty(value):
    return (value is None) or (value == "")


@task()
def get_property_from_dict(dict: dict, key: str):
    return dict[key]
