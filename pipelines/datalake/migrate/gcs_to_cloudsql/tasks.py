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
    # Aqui não temos como ser muito genéricos; queremos os backups mais recentes
    # de cada CNES. O formato dos nomes é o seguinte (ênfase no importante):
    # HISTÓRICO_PEPVITA_RJ/AP32/vitacare_historic_6808077_20250401_051630.bak
    #                                             ^^^^^^^ ^^^^^^^^
    #                                              CNES   YYYYMMDD
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


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def send_sequential_api_requests(
    most_recent_files: list, bucket_name: str, instance_name: str, limit_files: int
):
    # Caso queira limitar o número de arquivos para teste
    if limit_files is not None and limit_files > 0:
        log(f"[send_sequential_api_requests] Limiting to {limit_files} files")
        most_recent_files = most_recent_files[:limit_files]

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
        # Erro se o nome da database for inválido/esquisito
        utils.check_db_name(database_name)

        # Garante que não existe database com esse nome
        # https://cloud.google.com/sql/docs/sqlserver/create-manage-databases#delete
        # Chama a API e espera a operação terminar
        utils.call_and_wait("DELETE", f"/instances/{instance_name}/databases/{database_name}")

        log(
            "[send_sequential_api_requests] "
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


@task
def check_for_outdated_backups(most_recent_filenames: list):
    """
    Verifica se existe CNES com backups desatualizados em relação a data mais recente encontrada
    Envia uma mensagem ao discord

    Args:
        most_recent_files (list): Lista de filenames (dstrings) dos backups mais recentes por CNES
        Ex: ['HISTÓRICO_PEPVITA_RJ/AP10/vitacare_historic_2270250_20250601_051608.bak', ...]
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

    # 1. Definir a mais rceente data

    """
    Usando  Counter para verificar a data que mais aparece (a moda)
    Pegando pela data mais recente com max() não considera casos que apenas uma
    ou algumas poucas unidades possuem mais atualizada
    EX: De 10 unidades 9 possuem data 20250720 e apenas 1 20250722
    """
    date_counter = Counter(latest_dates)
    max_freq = max(date_counter.values())

    most_common_dates = [d for d, count in date_counter.items() if count == max_freq]
    most_recent_date = max(most_common_dates)
    log(f"[check_for_outdated_backups] Reference date set to: {most_recent_date}")

    # 2. Identificar unidades nao atualizadas
    outdated_cnes_info = []
    for cnes, latest_date_for_cnes in cnes_to_latest_date.items():
        if latest_date_for_cnes < most_recent_date:
            latest_date_for_cnes_br = f"{latest_date_for_cnes[-2:]}/{latest_date_for_cnes[4:6]}/{latest_date_for_cnes[:4]}"
            outdated_cnes_info.append(
                f"- CNES: {cnes}, Data do último backup: `{latest_date_for_cnes_br}`"
            )

    most_recent_date_br = f"{most_recent_date[-2:]}/{most_recent_date[4:6]}/{most_recent_date[:4]}"

    # 3. Cria e envia mensagem ao discord
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
            f"[check_for_outdated_backups] {len(outdated_cnes_info)} outdated CNES found. "
            "Sending message"
        )
    else:
        title = "🟢 Unidades com Backups Atualizados"
        message = (
            f"Todas as unidades estão atualizadas"
            f"A data esperada é: `{most_recent_date_br}` e todas as "
            "unidades possuem backup para esse mês"
        )
        log(
            "[check_for_outdated_backups] All CNES are up to date relative to the reference date. Sending success message"
        )

    send_message(
        title=title,
        message=message,
        monitor_slug="data-ingestion",
    )
