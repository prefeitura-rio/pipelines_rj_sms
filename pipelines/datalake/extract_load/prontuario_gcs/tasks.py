# -*- coding: utf-8 -*-
import csv
import os
import re
import shutil
import tarfile
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, storage
from pandas.errors import EmptyDataError

from pipelines.constants import constants
from pipelines.datalake.extract_load.prontuario_gcs.constants import (
    constants as prontuario_constants,
)
from pipelines.datalake.extract_load.prontuario_gcs.utils import (
    process_sql_file_streaming,
    read_table,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.googleutils import download_from_cloud_storage
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task
def create_temp_folders(folders) -> None:
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
    return


@task
def get_file(path, bucket_name, blob_prefix, environment, wait_for, blob_type) -> list:
    '''
    Faz o download de arquivo no bucket
    '''
    prefix = f'{blob_prefix}-{blob_type}'
    
    log(f"⬇️ Realizando download de {prefix}...")
    filename = download_from_cloud_storage(path, bucket_name, prefix) 
    
    log("✅ Download feito com sucesso.")
    return filename


@task
def unpack_files(tar_files: str, output_dir: str, environment) -> None:
    """ 
    Descomprime e deleta o arquivo tar.gz
    """
    log(f"📁 Descompactando {len(tar_files)} arquivo(s)...")

    for file in tar_files:
        log(f"Descompactando: {file}...")
        output_path = os.path.join(output_dir, os.path.basename(file).replace(".tar.gz", ""))
        with tarfile.open(file, "r:gz") as tar:
            tar.extractall(path=output_path)
            
        # Deleta o arquivo compactado para liberar armazenamento no flow 
        os.remove(file)
    log(f"✅ Arquivo descompactado em {output_dir}")
    return


@task
def extract_postgres_data(data_dir: str, wait_for, output_dir) -> None:
    log("📋️ Extraindo dados do arquivo hospub.sql...")
    target_tables = prontuario_constants.SELECTED_POSTGRES_TABLES.value

    upload_path = os.path.join(output_dir, "POSTGRES")
    os.makedirs(upload_path, exist_ok=True)
    postgres_folder = [
        folder_name for folder_name in os.listdir(data_dir) if "VISUAL" in folder_name
    ][0]

    sql_path = os.path.join(data_dir, postgres_folder, "hospub.sql")

    process_sql_file_streaming(sql_path, upload_path, target_tables)

    # Deleta o arquivo SQL para liberar armazenamento para o flow no Prefect
    os.remove(sql_path)
    return


@task
def extract_openbase_data(data_dir: str, output_dir: str, wait_for) -> str:
    log("📋️ Obtendo dados de arquivos OpenBase...")
    upload_path = os.path.join(output_dir, "OPENBASE")
    os.makedirs(upload_path, exist_ok=True)

    selected_tables = prontuario_constants.SELECTED_OPENBASE_TABLES.value
    openbase_folder = [
        folder_name for folder_name in os.listdir(data_dir) if "BASE" in folder_name
    ][0]

    openbase_path = os.path.join(data_dir, openbase_folder)

    log(f"Extraindo dados de {openbase_path}")

    tables = [x for x in os.listdir(openbase_path) if x.endswith("._S")]
    tables.sort()
    dictionaries = [x for x in os.listdir(openbase_path) if x.endswith("._Sd")]
    dictionaries.sort()

    data = zip(tables, dictionaries)
    tables_data = list(data)

    for table, dictionary in tables_data:
        if table.replace("._S", "") != dictionary.replace("._Sd", ""):
            log("Table and dictionary do not match: ", table, dictionary)

    dictionaries = {}
    for table, dictionary in tables_data:
        with open(
            f"{openbase_path}/{dictionary}",
            "r",
            encoding=prontuario_constants.DICTIONARY_ENCODING.value,
        ) as f:
            dictionary_data = f.readlines()
        # Ignore the 3 first lines
        dictionary_data = dictionary_data[3:]
        trimmed = [x.strip() for x in dictionary_data]
        separated = [x.split() for x in trimmed]

        structured_dictionary = {}
        acc_offset = 0
        for attrs in separated:
            name = attrs[0]
            _type = attrs[1]
            condition = attrs[2] if len(attrs) > 2 else None

            if "," in _type:
                _type = _type.split(",")[0]
            size = int(re.sub(r"\D", "", _type))

            structured_dictionary[name] = {"type": _type, "size": size, "offset": acc_offset}
            acc_offset += size
        dictionaries[table] = structured_dictionary

    csv_names = []
    for table, dictionary in tables_data:
        if not table in selected_tables:
            continue
        table_name = table.replace("._S", "").replace(f"{openbase_path}", "")
        structured_dictionary = dictionaries[table]
        csv_name = read_table(f"{openbase_path}/{table}", structured_dictionary, upload_path)
        csv_names.append(csv_name)

    # Deleta o diretório com os arquivos OpenBase para liberar armazenamento no Prefect
    shutil.rmtree(openbase_path)

    return csv_names


@task
def upload_to_datalake(
    upload_path, dataset_id, wait_for, environment, cnes, lines_per_chunk, base_type
) -> None:
    for folder in os.listdir(upload_path):
        if folder != base_type:
            continue
        files_dir = os.path.join(upload_path, folder)
        
        for file in os.listdir(files_dir):
            file_path = os.path.join(files_dir, file)
            try:
                csv_chunks = pd.read_csv(
                    file_path,
                    sep=",",
                    quotechar='"',
                    skipinitialspace=True,
                    dtype=str,
                    on_bad_lines="warn",
                    chunksize=lines_per_chunk,
                )
                for i, chunk in enumerate(csv_chunks):
                    df = pd.DataFrame(chunk)
                    if not df.empty:
                        # Remove possíveis pontos em nome de colunas que invialibizam o upload
                        cols_renamed = [col.replace(".", "") for col in df.columns]
                        df.rename(columns=dict(zip(df.columns, cols_renamed)), inplace=True)

                        df["loaded_at"] = datetime.now().isoformat()
                        df["cnes"] = cnes

                        table_name = f"{folder.lower()}_{file.replace('.csv', '')}"
                        upload_df_to_datalake.run(
                            df=df,
                            dataset_id=dataset_id,
                            table_id=table_name,
                            partition_column="loaded_at",
                            if_exists="append",
                            if_storage_data_exists="append",
                            source_format="csv",
                            csv_delimiter=",",
                        )
                        log(f"⬆️ Upload {i+1} de {table_name} feito com sucesso!")
            except EmptyDataError as e:
                log(f"⚠️ {file} está vazio.")
            except Exception as e:
                log(f"Erro no upload da tabela {file}")
                raise e
            
        # Deleta arquivos já upados para o bucket
        shutil.rmtree(files_dir)
    return


@task
def delete_temp_folders(folders, wait_for):
    for folder in folders:
        shutil.rmtree(folder)
    return


@task
def list_files_from_bucket(environment, bucket_name, folder):
    """ 
    Lista os arquivos no bucket e cria a relação de CNES-Prefix
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    files = bucket.list_blobs(prefix=f"{folder}/hospub")

    # Extrai o nome dos blobs no bucket
    files_path = [str(f.name) for f in files]
        
    # Faz a relação CNES - Prefixo
    cnes_prefix = {}
    
    for file in files:
        cnes_match = re.search(r"hospub-(\d+)", file)
        cnes = cnes_match.group(0)
        
        prefix_match = re.search(r".*hospub-\d+")
        prefix = prefix_match.group(0)
        
        cnes_prefix[cnes] = prefix
    print(cnes_prefix)
    return cnes_prefix


@task
def build_operator_parameters(
    files_per_cnes: dict,
    bucket_name: str,
    dataset_id: str,
    environment: str = "dev",
) -> list:
    """Gera lista de parâmetros para o(s) operator(s)."""
    return [
        {
            "environment": environment,
            "dataset": dataset_id,
            "bucket_name": bucket_name,
            "cnes": cnes,
            "blob_prefix": prefix,
            "rename_flow": True,
        }
        for cnes, prefix in files_per_cnes.items()
    ]


if __name__ == "__main__":
    files = list_files_from_bucket(environment=None, bucket_name="subhue_backups")
