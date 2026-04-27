# -*- coding: utf-8 -*-
import csv
import json
import os
import re
import shutil
import sys
import tarfile
from datetime import datetime, timedelta

import pandas as pd
import pytz
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from pandas.errors import EmptyDataError

from pipelines.datalake.extract_load.prontuario_gcs.constants import (
    constants as prontuario_constants,
)
from pipelines.datalake.extract_load.prontuario_gcs.utils import (
    find_openbase_folder,
    get_metadata_info,
    get_table_and_dictionary_files,
    load_all_dictionaries,
    parse_record,
    process_insert_statement,
    write_csv_header,
    openbase_write_csv_row
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
def get_file(path, bucket_name, blob_path, environment, wait_for, blob_type) -> list:
    """
    Faz o download do arquivo do bucket.
    """

    log(f"⬇️ Realizando download de {blob_path}...")
    filename = download_from_cloud_storage(path, bucket_name, blob_path)

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
                for file_name in tar.getnames():
                    if file_name in files_to_extract:
                        tar.extract(file_name, path=output_path)
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
        folder_name for folder_name in os.listdir(data_dir) if "sql" in folder_name
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

    with open(sql_path, "r", encoding="utf-8", errors="ignore", buffering=65536) as f:
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
                upload_file_to_native_table.run(
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
                upload_file_to_native_table.run(
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
            upload_file_to_native_table.run(
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
    tables_to_extract: list,
    wait_for,
) -> str:
    """
    Extrai dados de arquivos OpenBase e faz upload para o datalake.

    Args:
        data_dir: Diretório contendo os arquivos OpenBase
        output_dir: Diretório de saída para arquivos temporários
        cnes: Código CNES da unidade de saúde
        environment: Ambiente de execução
        lines_per_chunk: Número de linhas por arquivo CSV
        dataset_id: ID do dataset no datalake
        wait_for: Dependência do Prefect

    Returns:
        String vazia (mantido para compatibilidade)
    """
    log("📋 Obtendo dados de arquivos OpenBase...")

    # Configuração inicial
    upload_path = os.path.join(output_dir, "OPENBASE")
    os.makedirs(upload_path, exist_ok=True)

    # Localiza e prepara dados
    openbase_path = find_openbase_folder(data_dir)
    log(f"Extraindo dados de {openbase_path}")

    tables_data = get_table_and_dictionary_files(openbase_path)
    dictionaries = load_all_dictionaries(
        tables_data, openbase_path, prontuario_constants.DICTIONARY_ENCODING.value
    )

    for table, _ in tables_data:
        if table not in tables_to_extract:
            continue
        table_path = os.path.join(openbase_path, table)
        structured_dictionary = dictionaries[table]

        metadata, expected_length = get_metadata_info(structured_dictionary)
        table_name = table_path.split("/")[-1].split(".")[0]
        csv_path = os.path.join(upload_path, f"{table_name}.csv")

        create_file = True
        line_count = 0

        with open(table_path, "rb") as f:
            while True:
                if create_file:
                    write_csv_header(csv_path, metadata)
                    create_file = False

                rec = f.read(expected_length)

                if not rec:
                    # Upload final do arquivo restante
                    upload_file_to_native_table.run(
                        file=csv_path,
                        dataset_id=dataset_id,
                        environment=environment,
                        cnes=cnes,
                        base_type="openbase",
                    )
                    log(f"Extração finalizada para a tabela {table_name}.")
                    break

                row = parse_record(rec, structured_dictionary)
                openbase_write_csv_row(csv_path, row)
                line_count += 1

                if line_count >= lines_per_chunk:
                    upload_file_to_native_table.run(
                        file=csv_path,
                        dataset_id=dataset_id,
                        environment=environment,
                        cnes=cnes,
                        base_type="openbase",
                    )
                    log("Retomando extração...")
                    create_file = True
                    line_count = 0

    # Limpeza
    shutil.rmtree(openbase_path)
    return ""


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
        
    blobs = [b.name for b in bucket.list_blobs(prefix=f"{folder}/hospub")]

    last_files = {}
    pattern = re.compile(r'-(\d+)-(sql|openbase)-(\d{2}-\d{2}-\d{4}-\d{2}h\d{2}m)')

    # Dicionário temporário para controle de datas: {(cnes, file_type): data_dt}
    date_control = {}

    for path in blobs:
        match = pattern.search(path)
        if match:
            cnes, file_type, data_str = match.groups()
            data_dt = datetime.strptime(data_str, "%d-%m-%Y-%Hh%Mm")
            
            key_control = (cnes, file_type)
            
            # Verifica se é o mais recente para aquele CNES e file_type
            if key_control not in date_control or data_dt > date_control[key_control]:
                date_control[key_control] = data_dt
                
                # Inicializa o CNES no dicionário final se não existir
                if cnes not in last_files:
                    last_files[cnes] = {}
                
                # Atribui o path ao file_type correspondente
                last_files[cnes][file_type] = path
                
    # Retorna algo tipo { 'cnes': {'openbase': '/path/file', 'sql':'/path/file'} }
    return last_files

@task
def build_operator_parameters(
    last_files: dict,
    bucket_name: str,
    dataset_id: str,
    folder: str,
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
            "lines_per_chunk": chunk_size,
            "folder": folder,
            "skip_openbase": False,
            "openbase_blob": blob["openbase"],
            "skip_postgres": (
                True if cnes == "2273349" else False
            ),  # Este CNES não possui base POSTGRES
            "postgres_blob": blob["sql"]
        }
        for cnes, blob in last_files.items()
    ]


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def upload_file_to_native_table(
    environment: str,
    dataset_id: str,
    file: str,
    base_type: str,
    cnes: str,
):
    """Envia arquivo CSV para tabela nativa do bigquery.
    Utilizado para o envio de arquivos com número de colunas variável.

    Args:
        environment (str): Variável de ambiente
        dataset_id (str): Dataset de destino
        file (str): Caminho do arquivo CSV
        base_type (str): Tipo de base (openbase ou postgres)
        cnes (str): CNES da unidade de saúde
    """

    # Lê e prepara os dados para envio
    data_list = []
    csv.field_size_limit(sys.maxsize)

    with open(file, "r") as f:
        reader = csv.DictReader(
            [line.replace("\x00", "") for line in f],  # Remove null bytes
            delimiter="|",
            quotechar='"',
            skipinitialspace=True,
        )
        for row in reader:

            tamanho_bytes = sys.getsizeof(row)
            tamanho_mb = tamanho_bytes / (1024 * 1024)

            if tamanho_mb > 10:
                log("O dicionário tem mais de 10MB")
                log(row)
                raise Exception
            else:
                data_list.append(row)

    table = file.split("/")[-1].replace(".csv", "")
    lines = [
        {
            "cnes": cnes,
            "data": json.dumps(data),
            "loaded_at": datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None).isoformat(),
            "base_type": base_type,
        }
        for data in data_list
    ]

    if not lines:
        log(f"⚠️ O arquivo {file} está vázio. Não há linhas a inserir.")
        os.remove(file)
        return

    # Faz o envio dos dados para o BigQuery
    log(f"⬆️ Iniciando upload de {len(lines)} linhas para a tabela {table}...")
    
    project = "rj-sms" if environment == "prod" else "rj-sms-dev"
    client = bigquery.Client(project=project)
    dataset_ref = client.dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset)
        log(f"Dataset {dataset_id} criado.")

    table_ref = dataset_ref.table(table)

    schema = [
        bigquery.SchemaField("cnes", "STRING"),
        bigquery.SchemaField("data", "STRING"),
        bigquery.SchemaField("loaded_at", "DATETIME"),
        bigquery.SchemaField("base_type", "STRING"),
    ]

    try:
        client.get_table(table_ref)
    except NotFound:
        table_obj = bigquery.Table(table_ref, schema=schema)
        table_obj.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="loaded_at",
        )
        client.create_table(table_obj)
        log(f"Criada tabela {table_obj}")

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    try:
        load_job = client.load_table_from_json(lines, table_ref, job_config=job_config)
        result = load_job.result()
    except Exception as e:
        log(result)
        log(f"❌ Erro ao inserir linhas na tabela: {e}")
        raise e

    log(f"✅ Inserção de linhas feitas com sucesso")
    os.remove(file)

@task
def generate_current_folder(folder: str) -> str:
    """Se a pasta do bucket não for especificada, retorna ano-mes atual. Útil para extrações semanais"""
    if not folder:
        current_date = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        if current_date.day < 7:
            current_date = current_date - timedelta(month=1)
        folder = current_date.strftime("%Y-%m")
    return folder