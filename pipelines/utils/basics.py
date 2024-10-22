# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task


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
def is_null_or_empty(value):
    return (value is None) or (value == "")
