# -*- coding: utf-8 -*-
import fnmatch
from datetime import timedelta

import requests
from google.cloud import storage

import pipelines.datalake.migrate.gcs_to_cloudsql.utils as utils
from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def find_all_files_from_pattern(environment: str, file_pattern: str, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    log(f"Using pattern: {file_pattern}")
    files = []
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, file_pattern):
            files.append(blob)

    log(f"{len(files)} files were found")
    return files


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_most_recent_filenames(files):
    # Aqui não temos como ser muito genéricos; queremos os backups mais recentes
    # de cada CNES. O formato dos nomes é o seguinte (ênfase no importante):
    # HISTÓRICO_PEPVITA_RJ/AP32/vitacare_historic_6808077_20250401_051630.bak
    #                                             ^^^^^^^ ^^^^^^^^
    #                                              CNES   YYYYMMDD
    most_recent_dict = {}
    for file in files:
        info = utils.get_info_from_filename(filename=file.name)
        date = info["date"]
        cnes = info["cnes"]
        if cnes not in most_recent_dict or date > most_recent_dict[cnes][0]:
            most_recent_dict[cnes] = (date, file.name)

    log(f"Found {len(most_recent_dict.keys())} distinct CNES")
    most_recent_filenames = [filename for _, filename in list(most_recent_dict.values())]

    log("Sample of 5 files:\n" + "\n".join(most_recent_filenames[:5]))
    return most_recent_filenames


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def send_sequential_api_requests(most_recent_files: list, bucket_name: str, instance_name: str):
    most_recent_files = most_recent_files[:5]  # FIXME
    file_count = len(most_recent_files)
    log(f"[send_sequential_api_requests] Received {file_count} filename(s)")

    # Requisições precisam ser sequenciais porque a API só permite uma operação por vez
    # Caso contrário, dá erro HTTP 409 'Conflict'
    # https://cloud.google.com/sql/docs/troubleshooting#import-export

    # Situação:
    # - Queremos um backup sob um nome único para cada CNES (ex. `vitacare_historic_<CNES>`);
    # - O /import para uma database já existente não atualiza com novos dados, é um NO-OP;
    # - Não existe forma de renomear databases
    # Solução:
    # - Para cada arquivo:
    #   - Mandar um DELETE para a database, caso ela exista (e esperar concluir);
    #   - Rodar /import (e esperar concluir);
    #   - Torcer pra não dar nenhum erro, porque apagamos o backup anterior

    for i, file in enumerate(most_recent_files):
        if file is None or len(file) <= 0:
            log(f"[send_sequential_api_requests] Skipping file {i+1}/{file_count} (empty name)")
            continue

        # Prepara parâmetros da requisição
        full_file_uri = f"gs://{bucket_name}/{file}"
        info = utils.get_info_from_filename(filename=file)
        database_name = "_".join([info["name"], info["cnes"]])

        log("-" * 20)
        log(f"[send_sequential_api_requests] Attempting to delete database '{database_name}'...")

        # Garante que não existe database com esse nome
        # https://cloud.google.com/sql/docs/sqlserver/create-manage-databases#delete
        # Chama a API e espera a operação terminar
        utils.call_and_wait("DELETE", f"/instances/{instance_name}/databases/{database_name}")

        log(
            f"[send_sequential_api_requests] "
            + f"Attempting to import file {i+1}/{file_count}: "
            + f"'{full_file_uri}' (db '{database_name}')"
        )

        # Fazer o pedido de importação
        data = {
            "importContext": {
                "fileType": "BAK",
                "uri": full_file_uri,
                "database": database_name,
                "sqlServerImportOptions": {},
            }
        }
        # Chama a API e espera a operação terminar
        utils.call_and_wait("POST", f"/instances/{instance_name}/import", json=data)

    log("[send_sequential_api_requests] All done!")
