# -*- coding: utf-8 -*-
import os
from datetime import timedelta

import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def get_table_names():
    return [
        "paciente",
        "alergia",
        "atendimento",
        "boletim",
        "cirurgia",
        "classificacao_risco",
        "diagnostico",
        "exame",
        "internacao",
        "profissional",
        "m_estabelecimento",
        "produto_saldo_atual",
    ]


@task(max_retries=3, retry_delay=timedelta(seconds=120))
def download_table_data_to_parquet(
    db_url: str, table_name: str, start_datetime: str, end_datetime: str
) -> pd.DataFrame:
    df = pd.read_sql(
        f"""
        select *
        from basecentral.{table_name}
        where datahora between '{start_datetime}' and '{end_datetime}'
        """,
        db_url,
    )

    if not os.path.isdir("./tabledata"):
        log("Creating tabledata directory")
        os.mkdir("./tabledata")

    file_path = f"./tabledata/{table_name}-{start_datetime}-{end_datetime}.parquet"
    log(f"Saving table data to {file_path}")
    df.to_parquet(
        file_path,
        use_deprecated_int96_timestamps=True,
    )

    return file_path
