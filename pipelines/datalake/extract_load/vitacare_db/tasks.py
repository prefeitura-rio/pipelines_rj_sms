# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101, W0718
# flake8: noqa: E501
"""
Tasks for Vitacare db pipeline
"""
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

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
from pipelines.utils.googleutils import upload_to_cloud_storage
from pipelines.utils.tasks import (
    get_secret_key,
    load_file_from_bigquery,
    upload_to_datalake,
)


@task
def get_connection_string(environment: str):
    """
    Get the connection string for the Vitacare database.
    """

    if environment not in ["dev", "prod"]:
        error_message = "Invalid environment"
        log(error_message, level="error")
        raise FAIL(error_message)

    connection_string = {
        "host": get_secret_key.run(
            secret_path="/prontuario-vitacare-database",
            secret_name="DATABASE_HOST",
            environment=environment,
        ),
        "port": get_secret_key.run(
            secret_path="/prontuario-vitacare-database",
            secret_name="DATABASE_PORT",
            environment=environment,
        ),
        "user": get_secret_key.run(
            secret_path="/prontuario-vitacare-database",
            secret_name="DATABASE_USER",
            environment=environment,
        ),
        "password": get_secret_key.run(
            secret_path="/prontuario-vitacare-database",
            secret_name="DATABASE_PASSWORD",
            environment=environment,
        ),
    }

    log("Connection string retrieved successfully", level="info")

    return connection_string


@task
def get_bucket_name(env: str):
    """
    Get the bucket name based on the environment.
    """

    if env in ["dev", "local-prod", "prod"]:
        bucket_name = vitacare_constants.GCS_BUCKET.value[env]
    else:
        error_message = "Invalid environment"
        log(error_message, level="error")
        raise FAIL(error_message)

    return bucket_name


@task
def get_backup_file(bucket_name: str, cnes: str):
    """
    Get the backup filename from the bucket.
    """

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    blobs_iter = bucket.list_blobs(match_glob=f"HISTÓRICO_PEPVITA_RJ/**/vitacare_historic_{cnes}_*.bak")

    blobs = list(blobs_iter)
    log(f"Found {len(blobs)} backup files for CNES{cnes}:", level="info")
    for blob in blobs:
        log(f"- {blob.name}", level="info")

    # Check if there are any blobs
    if len(blobs) == 0:
        error_message = f"No backup files found for CNES{cnes}"
        log(error_message, level="error")
        raise FAIL(error_message)

    # Sort Blobs by Name
    blobs.sort(key=lambda x: x.name, reverse=True)
    most_recent_backup = blobs[0]

    folder_path = "/var/opt/mssql/backup"

    if not os.path.exists(folder_path):
        os.makedirs(folder_path, exist_ok=True)

    file_name = most_recent_backup.name.replace("/", "_")
    destination_file_name = os.path.join(folder_path, file_name)

    log(f"Backup file retrieved successfully: {destination_file_name}", level="info")

    return destination_file_name


@task
def get_backup_date(file_name: str):
    """
    Get the backup date from the file name.
    """

    date_str = file_name.split("_")[-2]
    time_str = file_name.split("_")[-1].removesuffix(".bak")
    date_obj = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")

    log(f"Backup date retrieved successfully: {date_obj}", level="info")

    return date_obj


@task
def get_database_name(cnes: str):
    return f"vitacare_{cnes}"


@task
def get_file_names():
    return [
        "atendimentos",
        "unidade",
        "equipes",
        "pacientes",
        "profissionais",
        "alergias",
        "condicoes",
        "encaminhamentos",
        "indicadores",
        "prescricoes",
        "procedimentos_clinicos",
        "solicitacao_exame",
        "vacinas",
    ]


@task
def create_temp_database(
    database_host: str,
    database_port: int,
    database_user: str,
    database_password: str,
    database_name: str,
    backup_file: str,
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
    RESTORE DATABASE {database_name} FROM DISK = '{backup_file}'
    WITH
        MOVE 'vitacare_historic' TO '/var/opt/mssql/data/{database_name}.mdf',
        MOVE 'vitacare_historic_log' TO '/var/opt/mssql/data/{database_name}_log.LDF'
    """

    log(f"Creating database {database_name} ...")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(restore_database_sql)
    while cursor.nextset():
        pass
    cursor.close()
    conn.close()


@task(max_retries=4, retry_delay=timedelta(seconds=30))
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

    # Force close all existing connections to the database
    close_connections_sql = f"""
    ALTER DATABASE {database_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    """
    delete_database_sql = f"DROP DATABASE {database_name}"

    log(f"Closing all connections to database {database_name} ...")
    cursor = conn.cursor()
    cursor.execute(close_connections_sql)
    while cursor.nextset():
        pass

    log(f"Deleting database {database_name} ...")
    cursor.execute(delete_database_sql)
    while cursor.nextset():
        pass
    cursor.close()
    conn.close()


@task(max_retries=4, retry_delay=timedelta(seconds=30))
def get_queries(database_name: str):
    return [
        f"SELECT * FROM {database_name}.dbo.ATENDIMENTOS",
        f"SELECT * FROM {database_name}.dbo.UNIDADE",
        f"SELECT * FROM {database_name}.dbo.EQUIPES",
        f"SELECT * FROM {database_name}.dbo.PACIENTES",
        f"SELECT * FROM {database_name}.dbo.PROFISSIONAIS",
        f"SELECT * FROM {database_name}.dbo.ALERGIAS",
        f"SELECT * FROM {database_name}.dbo.CONDICOES",
        f"SELECT * FROM {database_name}.dbo.ENCAMINHAMENTOS",
        f"SELECT * FROM {database_name}.dbo.INDICADORES",
        f"SELECT * FROM {database_name}.dbo.PRESCRICOES",
        f"SELECT * FROM {database_name}.dbo.PROCEDIMENTOS_CLINICOS",
        f"SELECT * FROM {database_name}.dbo.SOLICITACAO_EXAMES",
        f"SELECT * FROM {database_name}.dbo.VACINAS",
    ]


@task(max_retries=4, retry_delay=timedelta(seconds=30))
def create_parquet_file(
    database_host: str,
    database_port: int,
    database_user: str,
    database_password: str,
    database_name: str,
    sql: str,
    base_path: str,
    filename: str,
    backup_date: str,
    extract_if_table_is_missing: bool = False,
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

    try:
        df = pd.read_sql(sql, conn, dtype=str)

        id_cnes = database_name.removeprefix("vitacare_")

        log(f"Adding date metadata to {filename} ...", level="debug")

        df["id_cnes"] = id_cnes

        df["backup_created_at"] = str(backup_date)

        df["datalake_imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        log(f"Conforming header to datalake of {filename} ...")
        df.columns = remove_columns_accents(df)

        path = f"{base_path}/vitacare_historico_{id_cnes}_{filename}.parquet"

        df.to_parquet(path, index=False)

        conn.close()

        return path

    except Exception as e:
        if extract_if_table_is_missing:
            conn.close()
            log(
                f"Could not extract table {filename} because error: {e}. Skipping extraction.",
                level="warning",
            )

        else:
            conn.close()
            message = f"Could not extract table {filename} because error: {e}"
            log(message, level="error")
            raise FAIL(message) from e


@task(max_retries=4, retry_delay=timedelta(seconds=30))
def upload_many_to_datalake(
    input_path: list[str],
    dataset_id: str,
    source_format="parquet",
    if_exists="replace",
    if_storage_data_exists="replace",
    biglake_table=True,
    dataset_is_public=False,
    upload_if_table_is_missing: bool = False,
):
    """
    Uploads different tables to data lake.
    """

    for table in input_path:

        filename = Path(table).name
        table_id = (
            filename.replace("vitacare_historico_", "").split("_", 1)[1].removesuffix(".parquet")
        ) + "_historico"

        try:
            upload_to_datalake.run(
                input_path=table,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                source_format=source_format,
                if_exists=if_exists,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
            )
        except Exception as e:
            if upload_if_table_is_missing:
                log(
                    f"Could not upload table {filename} because error: {e}. Skipping upload.",
                    level="warning",
                )
            else:
                message = f"Could not upload table {filename} because error: {e}"
                log(message, level="error")
                raise FAIL(message) from e
    log("Files uploaded successfully", level="info")


@task
def generate_filters(last_update_start_date: str = None, last_update_end_date: str = None):
    """
    Generate filters based on the provided start and end dates.

    Args:
        last_update_start_date (str, optional): The start date for filtering. Defaults to None.
        last_update_end_date (str, optional): The end date for filtering. Defaults to None.

    Returns:
        tuple: A tuple containing the start and end dates if both are provided, otherwise None.
    """
    if last_update_start_date and last_update_end_date:
        return (last_update_start_date, last_update_end_date)

    return None


@task
def unzip_file(folder_path: str):
    """
    Unzip files in the given folder path.
    """
    errors = []
    for file in os.listdir(folder_path):
        if file.endswith(".zip"):
            try:
                shutil.unpack_archive(os.path.join(folder_path, file), folder_path)
            except Exception as e:
                log(f"Error unpacking file {file}: {e}", level="error")
                errors.append(file)

    if len(errors) > 0:
        log(f"Error unpacking {len(errors)} files", level="error")
        for error in errors:
            log(f"Error unpacking file {error}", level="error")

    bak_files = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".bak"):
                bak_files.append(os.path.join(root, file))

    log(f"Unzipped {len(bak_files)} files", level="info")

    return bak_files


@task
def check_filename_format(files: List[str]):
    """
    Check if the files have the correct format.
    """

    invalid_files = []
    valid_files = []

    for file in files:
        # convert string to pathlib.Path
        file_path = Path(file)
        cnes = file_path.name.split("_")[2]

        if not cnes.isdigit() or len(cnes) != 7:
            invalid_files.append(file)
        else:
            valid_files.append(file)

    if invalid_files:
        log(f"Existem {len(invalid_files)} arquivo(s) com CNES inválido(s)", level="warning")
        for file in invalid_files:
            file_path = Path(file).name
            log(f"INVALIDO: {file_path}", level="warning")
    else:
        log("SUCESSO: Todos os arquivos têm o formato correto", level="info")

    return valid_files


@task
def check_duplicated_files(files: List[str]):
    """
    Check if the files are duplicated in the given folder path.
    """

    # sort files by date to get the latest file
    files.sort(reverse=True)

    cnes_list = []
    unique_files = []
    duplicated_files = []

    for file in files:
        # convert string to pathlib.Path
        file_path = Path(file)
        cnes = file_path.name.split("_")[2]

        if cnes not in cnes_list:
            unique_files.append(file)
            cnes_list.append(cnes)
        else:
            duplicated_files.append(file)

    if duplicated_files:
        log(f"Existem {len(duplicated_files)} arquivo(s) duplicado(s)", level="warning")
        for file in duplicated_files:
            file_path = Path(file).name
            log(f"DUPLICADO: {file_path}", level="warning")
    else:
        log("SUCESSO: Nenhum arquivo duplicado encontrado", level="info")

    return unique_files


@task
def check_missing_or_extra_files(files: List[str], return_only_expected: bool = True):
    """
    Check if the files exist in the given folder path.
    """

    # retrieve estabelecimentos from bigquery
    dataset_name = "saude_dados_mestres"
    table_name = "estabelecimento"

    df_estabelecimentos_sms = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name=dataset_name,
        table_name=table_name,
    )

    df_estabelecimentos_vitacare = df_estabelecimentos_sms[
        (df_estabelecimentos_sms["prontuario_versao"] == "vitacare")
        & (df_estabelecimentos_sms["prontuario_episodio_tem_dado"] == "sim")
    ]

    estabelecimentos_esperados = set(df_estabelecimentos_vitacare["id_cnes"].unique())

    estabelecimentos_baixados = set(Path(file).name.split("_")[2] for file in files)

    # check if files are missing or extra
    estabelecimentos_faltando = estabelecimentos_esperados - estabelecimentos_baixados

    estabelecimentos_extra = estabelecimentos_baixados - estabelecimentos_esperados

    # log extra files
    if estabelecimentos_extra:
        log(
            f"Existe(m) {len(estabelecimentos_extra)} backup(s) extra(s) que não estão na lista de unidades da Vitacare com dados",
            level="warning",
        )
        for estabelecimento in estabelecimentos_extra:
            try:
                # Filter dataframe for current estabelecimento
                estabelecimento_info = df_estabelecimentos_sms[
                    df_estabelecimentos_sms["id_cnes"] == estabelecimento
                ]

                # Get estabelecimento details

                nome = estabelecimento_info["nome_limpo"].values[0]
                area_programatica = estabelecimento_info["area_programatica"].values[0]

                log(
                    f"EXTRA: {estabelecimento} - {nome} - AP {area_programatica}",
                    level="warning",
                )
            except Exception:
                log(
                    f"EXTRA: {estabelecimento} - Erro ao obter informações do estabelecimento",
                    level="warning",
                )
    else:
        log("SUCESSO: Nenhum estabelecimento extra encontrado", level="info")

    # log missing files
    if estabelecimentos_faltando:
        log(
            f"Existe(m) {len(estabelecimentos_faltando)} backup(s) faltante(s) que estão na lista de unidades da Vitacare com dados",
            level="warning",
        )
        for estabelecimento in estabelecimentos_faltando:
            estabelecimento_info = df_estabelecimentos_sms[
                df_estabelecimentos_sms["id_cnes"] == estabelecimento
            ]

            nome = estabelecimento_info["nome_limpo"].values[0]
            area_programatica = estabelecimento_info["area_programatica"].values[0]

            log(f"MISSING: {estabelecimento} - {nome} - AP {area_programatica}", level="warning")
    else:
        log("SUCESSO: Nenhum estabelecimento faltante encontrado", level="info")

    # get estabelecimentos to upload
    estabelecimentos_para_subir = (
        estabelecimentos_baixados - estabelecimentos_extra
        if return_only_expected
        else estabelecimentos_baixados
    )

    if return_only_expected:
        # drop extra files from files
        files = [
            file for file in files if Path(file).name.split("_")[2] in estabelecimentos_para_subir
        ]

    log(
        f"{len(files)} estabelecimentos para subir. Descartando {len(estabelecimentos_extra)} estabelecimentos extras",
        level="info",
    )

    return files


@task
def upload_backups_to_cloud_storage(files: List[str], staging_folder: str, bucket_name: str):
    """
    Upload backups to cloud storage.
    """
    # Move files to staging folder
    log(f"Moving {len(files)} files to staging folder", level="info")
    for file in files:
        filename = Path(file).name
        staging_path = Path(staging_folder) / filename
        shutil.copy(file, staging_path)

        log(f"Copied {filename} to staging folder", level="debug")

    log("Files copied successfully", level="info")

    # upload files to cloud storage
    log("Uploading files to cloud storage", level="info")

    blob_prefix = f"backups/{datetime.now().strftime('%Y-%m-%d')}"

    for file in files:
        log(
            f"Uploading {file} to cloud storage to {bucket_name}/{blob_prefix}",
            level="debug",
        )
        upload_to_cloud_storage(file, bucket_name, blob_prefix, if_exist="skip")

    log("Files uploaded successfully", level="info")

    return [str(Path(staging_folder) / Path(file).name) for file in files]
