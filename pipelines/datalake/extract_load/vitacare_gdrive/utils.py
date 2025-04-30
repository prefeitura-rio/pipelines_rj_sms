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
    if not sep or len(sep) <= 0:
        sep = ","

    csv_text = csv_text.replace("\r\n", "\n")

    first_line = csv_text.splitlines()[0]
    columns = first_line.split(sep)

    other_lines = csv_text.splitlines()[1:]

    max_cols = len(columns)
    for line in other_lines:
        line_columns = line.split(sep)

        if len(line_columns) > max_cols:
            max_cols = len(line_columns)

    diff = max_cols - len(columns)

    for i in range(diff):
        columns.append(f"complemento_{i}")

    new_first_line = sep.join(columns)
    new_csv_text = new_first_line + "\n" + "\n".join(other_lines)

    return new_csv_text


def fix_column_name(column_name: str) -> str:
    replace_from = [["(", ")"], [" ", "[", "]", "-", ".", ",", "/", "\\", "'", '"']]
    replace_to = ["", "_"]
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
    else:
        return ";"


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def download_file(bucket, file_name, extra_safe=True):
    blob = bucket.get_blob(file_name)
    size_in_bytes = blob.size
    size_in_mb = size_in_bytes / (1024 * 1024)
    log(f"[download_file] Beginning download of '{file_name}' ({size_in_mb:.1f} MB)")

    # Arquivos muito grandes estouram a memória do processo (OOMKilled)
    # Então precisamos quebrar esse arquivo em partes
    # [Ref] https://pipelines.dados.rio/sms/flow-run/1b988b6c-44d6-419c-b11f-2feabbb11ce2?logs

    csv_file = None
    sep = None
    # Caso o arquivo tenha >500 MB
    if size_in_mb > 500:
        try:
            # Download do arquivo direto para disco
            csv_file = tempfile.TemporaryFile()
            blob.download_to_file(csv_file)
        except Exception as e:
            log("error")  # FIXME: erro descritivo
            log(e)

        log(f"[download_file] Saved to temporary file.")

        # Pega primeira linha para detectar separador
        csv_file.seek(0)
        data = csv_file.readline()
        detected_encoding = chardet.detect(data)["encoding"]
        first_line = data.decode(detected_encoding)

        if len(first_line) <= 0:
            log(f"[download_file] First line is empty; error likely")
        sep = detect_separator(first_line)
        log(f"[download_file] Detected separator: '{sep}'")

        if extra_safe:
            # FIXME
            # csv_text = fix_csv(csv_text, sep)
            log(
                "[!] 'Safe download' (fix_csv) for large (>500 MB) files is not implemented yet",
                level="warning",
            )

    # Caso o arquivo tenha <= 500 MB
    else:
        # Download do arquivo em memória
        data = blob.download_as_bytes()
        detected_encoding = chardet.detect(data)["encoding"]
        csv_text = data.decode(detected_encoding)
        sep = detect_separator(csv_text)
        # Fix CSV
        if extra_safe:
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
