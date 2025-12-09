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
    handle_D,
    handle_J,
    handle_others,
    handle_U,
    process_insert_statement,
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
    """
    Faz o download de arquivo no bucket
    """
    prefix = f"{blob_prefix}-{blob_type}"

    log(f"⬇️ Realizando download de {prefix}...")
    filename = download_from_cloud_storage(path, bucket_name, prefix)

    log("✅ Download feito com sucesso.")
    return filename


@task
def unpack_files(
    tar_files: str,
    output_dir: str,
    files_to_extract: list,
    wait_for: any,
    exclude_origin: bool = False,
) -> None:
    """
    Descomprime e deleta arquivos tar.gz

    :param tar_files: Arquivos a serem descomprimidos
    :type tar_files: str
    :param output_dir: Diretório de saída dos arquivos
    :type output_dir: str
    :param files_to_extract: Arquivos que devem ser descomprimidos
    :type files_to_extract: list
    :param exclude_origin: Flag que indica que se deve excluir o arquivo tar.gz após a extração
    :type exclude_origin: bool
    """
    log(f"📁 Iniciando descompactação de arquivo(s)...")

    for file in tar_files:
        log(f"Descompactando: {file}...")

        output_path = os.path.join(output_dir, os.path.basename(file).replace(".tar.gz", ""))
        try:
            with tarfile.open(file, "r:gz") as tar:
                for file_in_tar in files_to_extract:
                    if not file_in_tar in tar.getnames():
                        continue
                    tar.extract(file_in_tar, path=output_path)
        except Exception as e:
            log(f"Erro ao descompactar o arquivo {file}: {e}")
    if exclude_origin:
        os.remove(file)
    log(f"✅ Arquivos descompactados")
    return


@task
def extract_postgres_data(
    data_dir: str,
    output_dir: str,
    lines_per_chunk: int,
    dataset_id: str,
    cnes: str,
    environment: str,
    sql_file: str,
    target_tables: list,
    wait_for: any,
) -> None:
    """Extrai linhas de tabelas específicas de um arquivo SQL e carrega em um dataset no BigQuery

    Args:
        data_dir (str): Diretório do arquivo .sql a ser extraído
        output_dir (str): Diretório de saída dos arquivos extraídos
        lines_per_chunk (int): Quantidade de linhas por upload
        dataset_id (str): Nome do dataset a ser carregado no BigQuery
        cnes (str): CNES referente ao arquivo extraído
        environment (str): Indicador de ambiente
        sql_file (str): Arquivo .sql a ser extraído
        target_tables (list): Tabelas que devem ser extraídas no arquivo
        wait_for (any): Variável para garantir a ordem do fluxo
    """

    log(f"📋️ Extraindo dados do arquivo {sql_file}...")

    upload_path = os.path.join(output_dir, "POSTGRES")
    os.makedirs(upload_path, exist_ok=True)

    postgres_folder = [
        folder_name for folder_name in os.listdir(data_dir) if "VISUAL" in folder_name
    ][0]

    sql_path = os.path.join(data_dir, postgres_folder, sql_file)

    # Converte lista de tabelas para conjunto para busca mais rápida
    if target_tables:
        target_tables = set(target_tables)

    current_insert = ""  # Buffer para acumular o comando INSERT atual
    processed_count = 0
    csv_name = None
    table_name = None

    if not os.path.exists(data_dir):
        return

    with open(sql_path, "r", encoding="utf-8", buffering=65536) as f:
        for line in f:

            # Remove espaços em branco no início e fim
            line = line.strip()

            # Ignora linhas vazias e comentários
            if not line or line.startswith("--") or line.startswith("/*") or line.startswith("#"):
                continue

            # Verifica se é início de um INSERT INTO
            if re.match(r"INSERT\s+INTO", line, re.IGNORECASE):
                # Se já havia um INSERT acumulado, processa ele
                if current_insert:
                    flag, csv_name, table_name = process_insert_statement(
                        current_insert, upload_path, target_tables
                    )
                    if flag:
                        processed_count += 1
                # Inicia novo INSERT
                current_insert = line
            else:
                # Continua acumulando o INSERT atual
                if current_insert:
                    current_insert += " " + line

            # Verifica se o INSERT está completo (termina com ;)
            if current_insert and current_insert.rstrip().endswith(";"):
                flag, csv_name, table_name = process_insert_statement(
                    current_insert, upload_path, target_tables
                )
                current_insert = ""
                if flag:
                    processed_count += 1

            # Verifica se trocou a tabela no INSERT mas ainda há valores
            # da tabela anterior a serem enviados
            if processed_count > 1 and table_name != previous_table:
                log(f"Linhas processadas: {processed_count}")
                log(f"Tabela atual: {table_name}")
                log(f"Tabela anterior: {previous_table}")
                upload_file_to_datalake.run(
                    file=previous_csv_name,
                    dataset_id=dataset_id,
                    cnes=cnes,
                    environment=environment,
                    base_type="postgres",
                )
                processed_count = 0
                log("Retomando extração...")

            # Verifica se bateu o limite de linhas por upload
            elif processed_count >= lines_per_chunk:
                upload_file_to_datalake.run(
                    file=csv_name,
                    dataset_id=dataset_id,
                    cnes=cnes,
                    environment=environment,
                    base_type="postgres",
                )
                processed_count = 0
                log("Retomando extração...")

            previous_csv_name = csv_name
            previous_table = table_name

    # Processa o último INSERT se houver
    if current_insert:
        flag, csv_name, table_name = process_insert_statement(
            current_insert, upload_path, target_tables
        )
        if flag:
            upload_file_to_datalake.run(
                file=csv_name,
                dataset_id=dataset_id,
                cnes=cnes,
                environment=environment,
                base_type="postgres",
            )

    # Deleta o arquivo SQL para liberar armazenamento para a flow run no Prefect
    os.remove(sql_path)
    return


@task
def extract_openbase_data(
    data_dir: str,
    output_dir: str,
    cnes: str,
    environment: str,
    lines_per_chunk: str,
    dataset_id: str,
    wait_for,
) -> str:
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

    for table, dictionary in tables_data:
        if not table in selected_tables:
            continue

        structured_dictionary = dictionaries[table]
        table_path = f"{openbase_path}/{table}"

        with open(table_path, "rb") as f:
            metadata = list(structured_dictionary.keys())
            metadata = sorted(metadata, key=lambda x: structured_dictionary[x]["offset"])
            last_col = metadata[-1]
            expected_length = (
                structured_dictionary[last_col]["size"]
                + structured_dictionary[last_col]["offset"]
                + 1
            )

            # Extrai o nome da tabela a partir do caminho do arquivo
            # Ex: ./data/hospub-2296306-BASE-31-08-2025-22h42m/alta_clinica._S.
            table_name = table_path.split("/")[-1].split(".")[0]
            csv_name = f"{upload_path}/{table_name}.csv"

            # Variáveis para controle de tamanho dos arquivos CSV
            create_file = True
            line_count = 0

            while True:
                if create_file:
                    # Cria o arquivo CSV para a respectiva tabela e adiciona a linha de colunas
                    with open(csv_name, "w") as csv_file:
                        csv_file.write(",".join(metadata) + "\n")
                    create_file = False

                rec = f.read(expected_length)

                if not rec:
                    # A extração da tabela chegou ao fim antes das 100_000 linhas
                    # Então faz o upload do que resta no arquivo.
                    try:
                        upload_file_to_datalake.run(
                            file=csv_name,
                            dataset_id=dataset_id,
                            environment=environment,
                            cnes=cnes,
                            base_type="openbase",
                        )
                    except FileNotFoundError:
                        pass
                    break

                row = {}
                for col, attrs in structured_dictionary.items():
                    field_bytes = rec[
                        attrs["offset"] : attrs["offset"] + attrs["size"]
                    ]  # .rstrip(b" \x00")
                    # Numerico
                    if attrs["type"].upper().startswith("J"):
                        value = handle_J(field_bytes, attrs)
                    # Alfanumerico
                    elif attrs["type"].upper().startswith("U"):
                        value = handle_U(field_bytes, attrs)
                    # Data
                    elif attrs["type"].upper().startswith("D"):
                        value = handle_D(field_bytes, attrs)
                    # Others
                    else:
                        value = handle_others(field_bytes, attrs)
                    row[col] = value

                # Escreve a linha extraída no CSV
                with open(csv_name, "a") as csv_file:
                    # Converte possíveis valores inteiros para string
                    line = [str(value) for value in row.values()]
                    csv_file.write(",".join(line) + "\n")
                line_count += 1

                if line_count >= lines_per_chunk:
                    upload_file_to_datalake.run(
                        file=csv_name,
                        dataset_id=dataset_id,
                        environment=environment,
                        cnes=cnes,
                        base_type="openbase",
                    )
                    create_file = True
                    line_count = 0

    # Libera armazenamento para a flow run
    shutil.rmtree(openbase_path)
    return


@task
def upload_file_to_datalake(
    file: str, dataset_id: str, cnes: str, base_type: str, environment: str
):
    try:
        df = pd.read_csv(
            file,
            sep=",",
            quotechar='"',
            skipinitialspace=True,
            dtype=str,
            on_bad_lines="warn",
        )
        if not df.empty:
            # Remove possíveis pontos em nome de colunas que invialibizam o upload
            cols_renamed = [col.replace(".", "") for col in df.columns]
            df.rename(columns=dict(zip(df.columns, cols_renamed)), inplace=True)

            df["loaded_at"] = datetime.now().isoformat()
            df["cnes"] = cnes
            table_name = file.split("/")[-1]
            table_name = table_name.replace(".csv", "")
            table_name = f"{base_type}_{table_name}"
            log(f"⬆️ Relizando upload de {file} com {df.shape[0]} linhas...")

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
            os.remove(file)
            del df
            log(f"✅ Upload {table_name} feito com sucesso!")
    except EmptyDataError as e:
        log(f"⚠️ {file} está vazio.")
    except Exception as e:
        log(f"Erro no upload da tabela {file}")
        raise e


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

    for file in files_path:
        cnes_matches = re.search(r"hospub-(\d+)", file)

        cnes_match = cnes_matches.group(0)  # Ex: hospub-2269945
        cnes = cnes_match.split("-")[1]  # 2269945

        prefix_match = re.search(r".*hospub-\d+", file)
        prefix = prefix_match.group(0)

        cnes_prefix[cnes] = prefix
    print(cnes_prefix)
    return cnes_prefix


@task
def build_operator_parameters(
    files_per_cnes: dict,
    bucket_name: str,
    dataset_id: str,
    chunk_size: int,
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
            "lines_per_chunk": chunk_size,
        }
        for cnes, prefix in files_per_cnes.items()
    ]


@task
def dumb_task(vars):
    return


if __name__ == "__main__":
    files = list_files_from_bucket(environment=None, bucket_name="subhue_backups")
