# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime
from pipelines.prontuarios.load_datalake.constants import (
    constants as datalake_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def return_endpoint(table_id: str):
    """
    Return endpoint based on table to be uploaded
    """
    return datalake_constants.ENDPOINT.value[table_id]


@task
def clean_null_values(df: pd.DataFrame, endpoint: str):
    """
    Delete null values from payload and prepare data to post
    """

    def normalize_list(row, endpoint):
        for dic in row:
            req_fields = [
                i
                for i in dic.keys()
                if i in datalake_constants.ENDPOINT_REQUIRED_FIELDS.value[endpoint]
            ]
            dic["is_valid"] = True
            for req_field in req_fields:
                if dic[req_field] is None:
                    dic["is_valid"] = False
                else:
                    pass
        new_row = [dic for dic in row if dic["is_valid"] is True]
        return new_row

    log(f"{len(df)} rows downloaded, treating null values")

    if endpoint == "mrg/professionals":
        df["cbo"] = df["cbo"].apply(lambda x: normalize_list(x, endpoint))
        df["conselho"] = df["conselho"].apply(lambda x: normalize_list(x, endpoint))
        payload = df[["id_profissional_sus", "cpf", "cns", "nome", "cbo", "conselho"]].to_dict(
            orient="records"
        )

    else:
        payload = normalize_list(df.to_dict(orient="records"), endpoint)

    return payload


@task
def fix_array_to_list(json_normalized: list):
    """
    Fix array field to list field
    """
    def transform_to_list(dic):
        dic_array = {}
        for k, v in dic.items():
            if (type(v) is np.ndarray):
                dic_array[k] = v.tolist()
            elif (type(v) is datetime.date):
                dic_array[k] = str(v)
            else:
                dic_array[k] = v
        return dic_array

    return list(map(transform_to_list, json_normalized))
