# -*- coding: utf-8 -*-

import pandas as pd

from pipelines.prontuarios.load_datalake.constants import (
    constants as datalake_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task


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

    if endpoint == "mrg/professionals":
        df["cbo"] = df["cbo"].apply(lambda x: normalize_list(x, endpoint))
        df["conselho"] = df["conselho"].apply(lambda x: normalize_list(x, endpoint))
        payload = df[["id_profissional_sus", "cpf", "cns", "nome", "cbo", "conselho"]].to_dict(
            orient="records"
        )

    else:
        payload = normalize_list(df.to_dict(orient="records"), endpoint)

    return payload
