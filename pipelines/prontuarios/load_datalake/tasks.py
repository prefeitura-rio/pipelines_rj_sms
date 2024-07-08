# -*- coding: utf-8 -*-

import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task


@task
def clean_null_values(df: pd.DataFrame):
    """
    Delete null values from payload and prepare data to post
    """

    def normalize_list(row):
        new_row = []
        for dict in row:
            req_field = [i for i in dict.keys() if i in ["id_cbo", "id_registro_conselho"]][0]
            if dict[req_field] is None:
                pass
            else:
                new_row.append(dict)
        return new_row

    df["cbo"] = df["cbo"].apply(normalize_list)
    df["conselho"] = df["conselho"].apply(normalize_list)

    payload = df[
        ["id_profissional_sus", "cpf", "cns", "nome", "cbo", "conselho"]
    ].to_dict(orient="records")

    return payload
