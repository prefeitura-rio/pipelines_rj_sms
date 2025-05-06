# -*- coding: utf-8 -*-
import datetime
import io
import tempfile

import chardet
import pytz
from tenacity import retry, stop_after_attempt, wait_fixed
from unidecode import unidecode

from pipelines.utils.logger import log


def fix_csv(csv_text: str, sep: str) -> str:
    log("Safe mode is on; fixing CSV text...")
    if not sep or len(sep) <= 0:
        sep = ","

    first_line = csv_text.splitlines()[0].strip()
    columns = first_line.split(sep)

    other_lines = csv_text.splitlines()[1:]

    max_cols = len(columns)
    for line in other_lines:
        line_columns = line.split(sep)
        max_cols = max(max_cols, len(line_columns))

    diff = max_cols - len(columns)
    for i in range(diff):
        columns.append(f"complemento_{i}")

    for i, line in enumerate(other_lines):
        new_line = line.strip()
        diff = max_cols - (new_line.count(sep) + 1)
        new_line += sep * diff
        other_lines[i] = new_line

    new_first_line = sep.join(columns) + "\n"
    new_csv_text = new_first_line + "\n".join(other_lines)

    return new_csv_text


def fix_csv_file(csv_file, sep: str):
    if csv_file.closed:
        log("Called `fix_csv_file` on closed file handle")
        return

    log("Safe mode is on; fixing CSV file...")
    if not sep or len(sep) <= 0:
        sep = ","

    # Como o arquivo é grande demais para carregar em memória,
    # criamos um segundo arquivo e fazemos transplante das
    # linhas uma a uma, padronizando no caminho
    new_csv_file = tempfile.TemporaryFile()

    # Lê primeira linha, separa em colunas
    csv_file.seek(0)
    data = csv_file.readline()
    detected_encoding = chardet.detect(data)["encoding"]
    first_line = data.decode(detected_encoding).strip()

    # Primeiro, queremos saber se todas as linhas possuem as mesmas colunas
    columns = first_line.split(sep)
    max_cols = len(columns)
    # Em teoria um `for` não vai carregar o arquivo inteiro em memória
    # [Ref] https://stackoverflow.com/a/6475407/4824627
    for data in csv_file:
        line = data.decode(detected_encoding)
        line_columns = line.split(sep)
        max_cols = max(max_cols, len(line_columns))

    # Se encontramos alguma linha com mais colunas que esperado,
    # adicionamos nomes de coluna dummy para facilitar o parsing
    diff = max_cols - len(columns)
    for i in range(diff):
        columns.append(f"complemento_{i}")

    new_first_line = sep.join(columns) + "\n"
    new_csv_file.write(new_first_line.encode("utf-8"))  # Melhor salvar como UTF-8

    csv_file.seek(0)
    csv_file.readline()
    # Para cada linha, removemos \r e garantimos que há o mesmo
    # número de valores que de colunas
    for data in csv_file:
        line = data.decode(detected_encoding)
        new_line = line.strip()
        # Padroniza número de campos por linha
        diff = max_cols - (new_line.count(sep) + 1)
        new_line += (sep * diff) + "\n"
        new_csv_file.write(new_line.encode("utf-8"))

    # .close() mata o arquivo temporário inicial
    csv_file.close()
    return new_csv_file


def fix_column_name(column_name: str) -> str:
    replace_from = [None] * 2
    replace_to = [None] * 2

    replace_from[0] = ["(", ")", "\r", "\n"]
    replace_to[0] = ""

    replace_from[1] = [" ", "[", "]", "-", ".", ",", "/", "\\", "'", '"']
    replace_to[1] = "_"

    for from_list, to_char in zip(replace_from, replace_to):
        for from_char in from_list:
            column_name = column_name.replace(from_char, to_char)

    return unidecode(column_name).lower()


def detect_separator(csv_text: str) -> str:
    first_line = csv_text.splitlines()[0]
    # Problema em potencial: presume que, se há mais ',' que ';',
    # o delimitador é ',', e vice-versa. Não necessariamente verdade
    if first_line.count(",") > first_line.count(";"):
        return ","
    if first_line.count(";") > first_line.count(","):
        return ";"
    log("[detect_separator] Ambiguous separator! Using ';'", level="warning")
    return ";"


def get_file_size(file_handle, seek_back_to=0) -> int:
    # Calcula o tamanho do arquivo aberto movendo o ponteiro para o último byte
    SEEK_END = 2
    file_size_bytes = file_handle.seek(0, SEEK_END)
    file_handle.seek(seek_back_to)
    return file_size_bytes


def format_bytes(bytes: int) -> str:
    gb = bytes / float(1024 * 1024 * 1024)
    if gb > 0.5:
        return f"{gb:.1f} GB"
    mb = bytes / float(1024 * 1024)
    if mb > 0.5:
        return f"{mb:.1f} MB"
    kb = bytes / float(1024)
    if kb > 0.5:
        return f"{kb:.1f} kB"
    return f"{bytes:.1f} bytes"


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def download_file(bucket, file_name, extra_safe=True):
    blob = bucket.get_blob(file_name)
    size_in_bytes = blob.size
    log(f"[download_file] Beginning download of '{file_name}' ({format_bytes(size_in_bytes)})")

    # Arquivos muito grandes estouram a memória do processo (OOMKilled)
    # Então precisamos quebrar esse arquivo em partes
    # [Ref] https://pipelines.dados.rio/sms/flow-run/1b988b6c-44d6-419c-b11f-2feabbb11ce2?logs

    csv_file = None
    sep = None
    size_in_mb = size_in_bytes / (1024 * 1024)
    MAX_SIZE_LOAD_TO_MEMORY_IN_MB = 250
    # Caso o arquivo seja grande demais
    if size_in_mb > MAX_SIZE_LOAD_TO_MEMORY_IN_MB:
        try:
            # Download do arquivo direto para disco
            csv_file = tempfile.TemporaryFile()
            blob.download_to_file(csv_file)
        except Exception as e:
            log("[download_file] Error downloading file to disk")
            raise e

        downloaded_file_size = get_file_size(csv_file)
        log(f"[download_file] Saved to temporary file ({format_bytes(downloaded_file_size)}).")

        # Pega primeira linha para detectar separador
        data = csv_file.readline()
        detected_encoding = chardet.detect(data)["encoding"]
        first_line = data.decode(detected_encoding)

        if len(first_line) <= 0:
            log(f"[download_file] First line is empty; error likely", level="warning")
        sep = detect_separator(first_line)
        log(f"[download_file] Detected separator: '{sep}'")

        if extra_safe:
            csv_file = fix_csv_file(csv_file, sep)
        csv_file.seek(0)

    # Caso o arquivo seja pequeno o suficiente
    else:
        # Download do arquivo em memória
        data = blob.download_as_bytes()
        detected_encoding = chardet.detect(data)["encoding"]
        csv_text = data.decode(detected_encoding)
        sep = detect_separator(csv_text)
        # Evita erros independentemente da flag se o arquivo for pequeno o suficiente
        csv_text = fix_csv(csv_text, sep)
        csv_file = io.StringIO(csv_text)

    # Retorna tupla com:
    # - Handle do buffer aberto do CSV (arquivo real ou StringIO, a depender do tamanho)
    # - Separador detectado
    # - Metadado a ser adicionado ao dataframe final
    return (
        csv_file,
        sep,
        {
            "_source_file": file_name,
            "_extracted_at": blob.updated.astimezone(pytz.timezone("America/Sao_Paulo")),
            "_loaded_at": datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo")),
        },
    )
