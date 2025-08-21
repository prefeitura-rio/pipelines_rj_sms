# -*- coding: utf-8 -*-
import fnmatch
from collections import Counter
from datetime import timedelta

from google.cloud import storage

import pipelines.datalake.migrate.gcs_to_cloudsql.utils as utils

# Mantendo o decorador customizado original
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def find_all_filenames_from_pattern(environment: str, file_pattern: str, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    log(f"Using pattern: {file_pattern}")
    files = [blob.name for blob in blobs if fnmatch.fnmatch(blob.name, file_pattern)]
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
    most_recent_filenames = [filename for _, filename in sorted(list(most_recent_dict.values()))]
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
    if not file:
        log("Skipping task due to empty filename.")
        return

    full_file_uri = f"gs://{bucket_name}/{file}"
    info = utils.get_info_from_filename(filename=file)
    database_name = "_".join([info["name"], info["cnes"]])

    log("-" * 20)
    log(f"Processing file: {file}")
    utils.check_db_name(database_name)

    log(f"Attempting to delete database '{database_name}'...")
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
    # (Função inalterada, omitida por brevidade)
    pass


# =========== NOVAS TASKS ADICIONADAS ABAIXO ===========


@task
def start_instance(instance_name: str):
    """Busca a config da instância, garante que a política seja 'ALWAYS' e aguarda a operação."""
    log(f"Ensuring instance '{instance_name}' is started...")

    instance_data = utils.get_instance(instance_name)
    current_policy = instance_data.get("settings", {}).get("activationPolicy")

    if current_policy == "ALWAYS" and instance_data.get("state") == "RUNNABLE":
        log(f"Instance '{instance_name}' is already running.")
        return

    # Modifica apenas a política de ativação na configuração existente
    instance_data["settings"]["activationPolicy"] = "ALWAYS"

    log(f"Sending PATCH to start instance '{instance_name}'...")
    utils.call_and_wait("PATCH", f"/instances/{instance_name}", json=instance_data)
    log(f"Instance '{instance_name}' is running.")


@task
def stop_instance(instance_name: str):
    """Busca a config da instância, garante que a política seja 'NEVER' e aguarda a operação."""
    log(f"Ensuring instance '{instance_name}' is stopped...")

    instance_data = utils.get_instance(instance_name)
    current_policy = instance_data.get("settings", {}).get("activationPolicy")

    if current_policy == "NEVER":
        log(f"Instance '{instance_name}' is already set to NEVER policy.")
        return

    # Modifica apenas a política de ativação na configuração existente
    instance_data["settings"]["activationPolicy"] = "NEVER"

    log(f"Sending PATCH to stop instance '{instance_name}'...")
    utils.call_and_wait("PATCH", f"/instances/{instance_name}", json=instance_data)
    log(f"Instance '{instance_name}' stopped.")
