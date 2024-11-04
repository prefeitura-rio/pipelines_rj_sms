# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Tasks for Vitacare db pipeline
"""
from datetime import datetime

import pandas as pd
import pytz
from google.cloud import storage
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.vitacare_db.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_db.utils import create_db_connection
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.tasks import upload_to_datalake


@task
def get_bucket_name(env: str):
    """
    Get the bucket name based on the environment.
    """

    if env in ["dev", "prod"]:
        bucket_name = vitacare_constants.GCS_BUCKET.value[env]
    else:
        raise FAIL("Invalid environment")

    return bucket_name


@task
def get_backup_filename(bucket_name: str, cnes: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix="backups", match_glob=f"*/vitacare_historic_{cnes}**.bak")

    return list(blobs)[0].name.removeprefix("backups/")


@task
def get_database_name(cnes: str):
    return f"vitacare_{cnes}"


@task
def create_temp_database(
    database_host: str,
    database_port: int,
    database_user: str,
    database_password: str,
    database_name: str,
    backup_filename: str,
):
    conn = create_db_connection(
        database_host=database_host,
        database_port=database_port,
        database_user=database_user,
        database_password=database_password,
        database_name="master",
        autocommit=True,
    )

    restore_database_sql = f"""
    RESTORE DATABASE {database_name} FROM DISK = '/var/opt/mssql/backup/{backup_filename}'
    WITH
        MOVE 'vitacare_historic' TO '/var/opt/mssql/data/{database_name}.mdf',
        MOVE 'vitacare_historic_log' TO '/var/opt/mssql/data/{database_name}_log.LDF'
    """

    log(f"Creating database {database_name} ...")
    conn.execute(restore_database_sql)
    conn.close()


@task
def delete_temp_database(
    database_host: str,
    database_port: int,
    database_user: str,
    database_password: str,
    database_name: str,
):
    conn = create_db_connection(
        database_host=database_host,
        database_port=database_port,
        database_user=database_user,
        database_password=database_password,
        database_name="master",
        autocommit=True,
    )
    delete_database_sql = f"DROP DATABASE {database_name}"

    log(f"Deleting database {database_name} ...")
    conn.autocommit = True
    conn.execute(delete_database_sql)
    conn.close()


@task
def get_queries(database_name: str):
    return [
        f"SELECT * FROM {database_name}.dbo.ATENDIMENTOS",
        f"SELECT * FROM {database_name}.dbo.UNIDADE",
        f"SELECT * FROM {database_name}.dbo.EQUIPES",
        f"SELECT * FROM {database_name}.dbo.PACIENTES",
        f"SELECT * FROM {database_name}.dbo.PROFISSIONAIS",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.ALERGIAS B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.CONDICOES B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.ENCAMINHAMENTOS B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.INDICADORES B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.PRESCRICOES B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.PROCEDIMENTOS_CLINICOS B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.SOLICITACAO_EXAMES B ON A.ACTO_ID = B.ACTO_ID",
        f"SELECT B.* FROM {database_name}.dbo.ATENDIMENTOS A JOIN {database_name}.dbo.VACINAS B ON A.ACTO_ID = B.ACTO_ID",
    ]


@task
def create_parquet_file(
    database_host: str,
    database_port: int,
    database_user: str,
    database_password: str,
    database_name: str,
    sql: str,
    base_path: str,
    filename: str,
):
    log(f"Creating database connection to {filename} ...")
    conn = create_db_connection(
        database_host=database_host,
        database_port=database_port,
        database_user=database_user,
        database_password=database_password,
        database_name="master",
    )
    tz = pytz.timezone("Brazil/East")

    log(f"Making SQL query of {filename} ...")
    df = pd.read_sql(sql, conn)

    log(f"Fixing parquet type of {filename} ...", level="debug")
    for col in df.columns:
        df[col] = df[col].astype(str)

    log(f"Adding date metadata to {filename} ...", level="debug")
    df["id_cnes"] = database_name.removeprefix("vitacare_")
    df["imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    log(f"Conforming header to datalake of {filename} ...")
    df.columns = remove_columns_accents(df)

    path = f"{base_path}/vitacare_historico_{filename}.parquet"

    df.to_parquet(path, index=False)

    conn.close()

    return path


@task
def upload_many_to_datalake(
    input_path: list[str],
    dataset_id: str,
    source_format="parquet",
    if_exists="replace",
    if_storage_data_exists="replace",
    biglake_table=True,
    dataset_is_public=False,
):
    """
    Uploads different tables to data lake.
    """

    for table in input_path:
        table_name = f'{table.split("_")[-1].removesuffix(".parquet")}_historico'
        upload_to_datalake.run(
            input_path=table,
            dataset_id=dataset_id,
            table_id=table_name,
            dump_mode="append",
            source_format=source_format,
            if_exists=if_exists,
            if_storage_data_exists=if_storage_data_exists,
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
        )
    log("Files uploaded successfully", level="info")
