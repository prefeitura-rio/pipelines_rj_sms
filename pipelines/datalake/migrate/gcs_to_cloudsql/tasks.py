# -*- coding: utf-8 -*-
import fnmatch
from collections import Counter
from datetime import timedelta

from google.cloud import storage

import pipelines.datalake.migrate.gcs_to_cloudsql.utils as utils
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def find_all_filenames_from_pattern(environment: str, file_pattern: str, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    log(f"Using pattern: {file_pattern}")
    files = []
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, file_pattern):
            files.append(blob.name)

    log(f"{len(files)} files were found")
    return files


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_most_recent_filenames(files):
    most_recent_dict = {}
    for file in files:
        info = utils.get_info_from_filename(filename=file)
        date = info["date"]
        cnes = info["cnes"]
        if cnes not in most_recent_dict or date > most_recent_dict[cnes][0]:
            most_recent_dict[cnes] = (date, file)

    log(f"Found {len(most_recent_dict.keys())} distinct CNES")
    most_recent_filenames = [filename for _, filename in list(most_recent_dict.values())]

    log("Sample of 5 files:\n" + "\n".join(most_recent_filenames[:5]))
    return most_recent_filenames


@task
def limit_files_task(files: list, limit: int):
    if limit is not None and limit > 0:
        log(f"Limiting to {limit} files")
        return files[:limit]
    return files


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def process_single_database_backup(file: str, bucket_name: str, instance_name: str):
    if file is None or len(file) <= 0:
        log("Skipping task due to empty filename.")
        return

    full_file_uri = f"gs://{bucket_name}/{file}"
    info = utils.get_info_from_filename(filename=file)
    database_name = "_".join([info["name"], info["cnes"]])

    log("-" * 20)
    log(f"Processing file: {file}")
    log(f"Attempting to delete database '{database_name}'...")
    utils.check_db_name(database_name)

    utils.call_and_wait("DELETE", f"/instances/{instance_name}/databases/{database_name}")

    log(f"Attempting to import file '{full_file_uri}' (db '{database_name}')")

    data = {
        "importContext": {
            "fileType": "BAK",
            "uri": full_file_uri,
            "database": database_name,
            "sqlServerImportOptions": {},
        }
    }
    utils.call_and_wait("POST", f"/instances/{instance_name}/import", json=data)
    log(f"Successfully processed database: {database_name}")


@task
def check_for_outdated_backups(most_recent_filenames: list):
    """
    Verifica se existe CNES com backups desatualizados em relação a data mais recente encontrada
    Envia uma mensagem ao discord

    Args:
        most_recent_files (list): Lista de filenames (dstrings) dos backups mais recentes por CNES
        Ex: ['HISTÓRIICO_PEPVITA_RJ/AP10/vitacare_historic_2270250_20250601_051608.bak', ...]
    """
    if not most_recent_filenames:
        log("[check_for_outdated_backups] No recent files found to check.")
        return

    cnes_to_latest_date = {}
    latest_dates = []

    for filename in most_recent_filenames:
        info = utils.get_info_from_filename(filename=filename)
        cnes = info["cnes"]
        date_str = info["date"]
        cnes_to_latest_date[cnes] = date_str
        latest_dates.append(date_str)

    date_counter = Counter(latest_dates)
    max_freq = max(date_counter.values())

    most_common_dates = [d for d, count in date_counter.items() if count == max_freq]
    most_recent_date = max(most_common_dates)
    log(f"[check_for_outdated_backups] Reference date set to: {most_recent_date}")

    outdated_cnes_info = []
    for cnes, latest_date_for_cnes in cnes_to_latest_date.items():
        if latest_date_for_cnes < most_recent_date:
            latest_date_for_cnes_br = f"{latest_date_for_cnes[-2:]}/{latest_date_for_cnes[4:6]}/{latest_date_for_cnes[:4]}"  # noqa
            outdated_cnes_info.append(
                f"- CNES: {cnes}, Data do último backup: `{latest_date_for_cnes_br}`"
            )

    most_recent_date_br = f"{most_recent_date[-2:]}/{most_recent_date[4:6]}/{most_recent_date[:4]}"

    title = ""
    message = ""
    if outdated_cnes_info:
        title = "🔴 Unidades com Backups Desatualizados"
        message = (
            f"Unidades com backups desatualizados "
            f"A data esperada é: `{most_recent_date_br}`, "
            f"mas os backups mais recentes para as unidades listadas são de datas anteriores:\n"
            + "\n".join(outdated_cnes_info)
        )
        log(
            f"[check_for_outdated_backups] {len(outdated_cnes_info)} outdated CNES found. Sending message"  # noqa
        )
    else:
        title = "🟢 Unidades com Backups Atualizados"
        message = (
            f"Todas as unidades estão atualizadas"
            f"A data esperada é: `{most_recent_date_br}` e todas as unidades possuem backup para esse mês"  # noqa
        )
        log(
            "[check_for_outdated_backups] All CNES are up to date relative to the reference date. Sending success message"  # noqa
        )

    send_message(
        title=title,
        message=message,
        monitor_slug="data-ingestion",
    )