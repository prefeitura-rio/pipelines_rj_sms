# -*- coding: utf-8 -*-
# [ Exportação + upload adaptado do .ipynb da Vitoria 🪦🥀 ]
import datetime
import os
import re
import shutil
import tempfile
import unicodedata
import zipfile
from time import sleep

import pandas as pd
import pytz

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake

from .utils import (
    authenticated_get,
    authenticated_post,
    download_gcs_to_file,
    format_reference_date,
    inverse_exponential_backoff,
)


@task()
def request_export(uri: str, environment: str = "dev") -> str:
    log(f"Requesting export of URI '{uri}'")
    json = authenticated_post("/export/", {"gcs_uri": uri}, enviroment=environment)
    log(json)
    if "success" not in json or not json["success"]:
        raise ValueError("Failed to request export!")

    return json["id"]  # Retorna UUID da task criada


@task()
def check_export_status(uuid: str, environment: str = "dev") -> str:
    # Aqui, vamos fazer um Inverse Exponential Backoff
    # Isto é, começamos esperando um tempo alto (INITIAL_BACKOFF_SECONDS) e,
    # a cada iteração, diminuímos o tempo de espera por algum fator até
    # atingir o tempo mínimo de espera (MIN_BACKOFF_SECONDS)
    # A função é da forma max(INITIAL/BASE^x, MIN)
    INITIAL_BACKOFF_SECONDS = 1200  # 1200 = 20min
    MIN_BACKOFF_SECONDS = 60
    BACKOFF_BASE = 1.3
    # Máximo de vezes que vamos conferir o status da tarefa até desistir dela
    MAX_ATTEMPTS = 200

    # Para calcular o tempo total esperado, use o WolframAlpha (https://wolframalpha.com/)
    # Insira: `(sum{x=0,MAX_ATTEMPTS} max(INITIAL/BASE^x, MIN))sec`
    # Ex.: `(sum{x=0,200} max(1200/1.3^x, 60))sec` retorna ~4h 32min
    # O GDB de abril/2025 do SIH levou ~3 horas pra baixar e exportar; então precisamos de
    # pelo menos isso

    attempts = -1
    while True:
        log(f"{attempts+1} / {MAX_ATTEMPTS} ({((attempts+1)/MAX_ATTEMPTS)*100:.1f}%)")
        json = authenticated_get(f"/check/{uuid}", enviroment=environment)
        log(json)
        status = json["status"]
        # Task terminou e foi bem sucedida
        if status == "SUCCESS":
            return json["result"]["output"]
        # Task terminou e falhou
        elif status == "FAILURE":
            raise Exception("Extraction status was FAILURE")
        # Task ainda não foi exectada
        elif status == "PENDING":
            # TODO: permitir uma espera maior?
            # porque se estamos em PENDING, sabe-se la quantas tasks
            # estão na nossa frente
            pass
        # Só sobra como possível status PROGRESS (etapa da execução)

        attempts += 1
        if attempts >= MAX_ATTEMPTS:
            raise Exception(
                f"Desistiu de aguardar exportação com ID {uuid}. "
                "A exportação provavelmente ainda está sendo executada pela API! "
                "Após seu término, você pode re-executar este flow com os mesmos "
                "parâmetros, mas usando `from_zip=true`, para que ele continue a "
                "executar a partir da extração do ZIP exportado."
            )

        # Esperamos cada vez menos tempo para conferir o status da task
        delay = inverse_exponential_backoff(
            attempts,
            INITIAL_BACKOFF_SECONDS,
            BACKOFF_BASE,
            MIN_BACKOFF_SECONDS,
        )
        log(f"Sleeping for {int(delay)}s")
        sleep(delay)


@task()
def extract_compressed(uri: str, environment: str = "dev") -> str:
    # O flow pode ser chamado sem extração do GDB em si, só para
    # upar de um zip previamente criado; assim, aqui conferimos
    # por via das dúvidas se a URI foi enviada com .GDB sem querer
    if uri.lower().endswith(".gdb"):
        # Se sim, trocamos por .zip
        uri = f"{uri[:-4]}.zip"
    log(f"Downloading file at '{uri}'")

    # Se ainda assim chegamos aqui sem um ZIP, avisa, mas tenta seguir
    # Vai que é um .tar.gz ou algo doido assim e funciona ¯\_(ツ)_/¯
    if not uri.lower().endswith(".zip"):
        log("This task expects ZIP files; this will likely throw error", level="warning")

    file_hdl = download_gcs_to_file(uri)
    temp_dir_path = tempfile.mkdtemp()

    try:
        with zipfile.ZipFile(file_hdl, "r") as zip_ref:
            log(f"Extracting ZIP to temp folder: '{temp_dir_path}'")
            zip_ref.extractall(temp_dir_path)
    except Exception as e:
        log("Error extracting zip file!", level="error")
        raise e
    finally:
        # Handle de TemporaryFile, então close() apaga o arquivo automaticamente
        file_hdl.close()
    return temp_dir_path


@task()
def upload_to_bigquery(
    path: str, dataset: str, uri: str, refdate: str | None, environment: str = "dev"
) -> str:
    files = [
        file
        for file in os.listdir(path)
        if (os.path.isfile(os.path.join(path, file)) and file.endswith(".csv"))
    ]
    log(f"Files extracted ({len(files)}): {files[:5]} (first 5)")

    # Lemos o CSV em pedaços para não estourar a memória
    LINES_PER_CHUNK = 100_000
    for i, file in enumerate(files):
        table_name = file.removesuffix(".csv").strip()
        # Remove potenciais caracteres problemáticos em nomes de tabelas
        table_name = re.sub(
            r"_{3,}",
            "__",  # Limita underlines consecutivos a 2
            re.sub(r"[^A-Za-z0-9_]", "_", table_name),
        )

        csv_path = os.path.join(path, file)
        csv_reader = pd.read_csv(
            csv_path, dtype="unicode", na_filter=False, chunksize=LINES_PER_CHUNK
        )
        log(f"Uploading {table_name} ({i+1}/{len(files)})")

        # Iteramos por cada pedaço do CSV
        for j, chunk in enumerate(csv_reader):
            # Cria um dataframe a partir do chunk
            df = pd.DataFrame(chunk)
            log(f"Reading chunk #{j+1} (at most {LINES_PER_CHUNK} lines)")

            # (acho que isso nem é mais possível, porque não entraria no `for`)
            if df.empty:
                log(f"{table_name} is empty; skipping")
                continue

            column_mapping = dict()
            existing_columns = set()
            for col in df.columns:
                # Remove tudo que não for letra, dígito e underline
                new_col = re.sub(
                    r"_{3,}",
                    "__",  # Limita underlines consecutivos a 2
                    re.sub(r"[^A-Za-z0-9_]", "_", unicodedata.normalize("NFKD", col)),
                )
                # Garante que não há múltiplas colunas com mesmo nome
                new_col_no_repeats = new_col
                repeat_count = 0
                while new_col_no_repeats in existing_columns:
                    repeat_count += 1
                    new_col_no_repeats = f"{col}_{repeat_count}"
                existing_columns.add(new_col_no_repeats)
                # Cria mapeamento do nome da coluna original -> tratado
                column_mapping[col] = new_col_no_repeats
            log(column_mapping)

            # Substitui nomes de colunas pelos nomes tratados
            df.rename(columns=column_mapping, inplace=True)

            # Metadados
            df["_source_file"] = uri
            df["_loaded_at"] = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
            df["_data_particao"] = format_reference_date(refdate, uri)

            log(
                f"Uploading table '{table_name}' from DataFrame: "
                f"{len(df)} rows; columns {list(df.columns)}"
            )
            attempt = 0
            MAX_UPLOAD_ATTEMPTS = 5
            while True:
                attempt += 1
                try:
                    # Chama a task de upload
                    upload_df_to_datalake.run(
                        df=df,
                        dataset_id=dataset,
                        table_id=table_name,
                        partition_column="_data_particao",
                        if_exists="append",
                        if_storage_data_exists="append",
                        source_format="csv",
                        csv_delimiter=",",
                    )
                    break
                # Aqui deu erro de conexão comigo algumas vezes:
                # > socket.gaierror: [Errno -3] Temporary failure in name resolution
                # > httplib2.error.ServerNotFoundError:
                #     Unable to find the server at cloudresourcemanager.googleapis.com
                # > http.client.RemoteDisconnected: Remote end closed connection without response
                # > "Something went wrong while setting permissions for BigLake service account [...]"
                # Então pescamos por um erro e tentamos mais uma vez por via das dúvidas
                except Exception as e:
                    # Se já tentamos N vezes, desiste e dá erro
                    if attempt > MAX_UPLOAD_ATTEMPTS:
                        raise e
                    # Senão, pausa por uns segundos e tenta de novo
                    log(f"{repr(e)}; sleeping for 5s and retrying", level="warning")
                    sleep(5)

    # Apaga pasta temporária
    shutil.rmtree(path, ignore_errors=True)
