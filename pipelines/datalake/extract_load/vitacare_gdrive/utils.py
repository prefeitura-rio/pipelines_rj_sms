# -*- coding: utf-8 -*-
import datetime
import io

import chardet
import pytz
from tenacity import retry, stop_after_attempt, wait_fixed
from unidecode import unidecode

from pipelines.utils.logger import log


def fix_csv(csv_text: str, sep: str) -> str:
    csv_text = csv_text.replace("\r\n", "\n")

    first_line = csv_text.splitlines()[0]

    columns = first_line.split(sep)

    other_lines = csv_text.splitlines()[1:]

    max_cols = len(columns)
    for line in other_lines:
        line_columns = line.split(",")  # FIXME: `sep` ao invés de ","? -Avellar

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
        # Gera nome do arquivo temporário
        timestamp = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo")).isoformat()
        file_name_no_slash = file_name.replace("/", "-").replace("\\", "-")
        tmp_file_name = f"{timestamp}--{file_name_no_slash}"

        try:
            # Download do arquivo direto para disco
            with open(f"/tmp/{tmp_file_name}", "wb+") as file_obj:
                blob.download_to_file(file_obj)
        except Exception as e:
            log("error")
            log(e)

        log(f"[download_file] Saved to local file: '/tmp/{tmp_file_name}'")

        # TODO: Está lidando corretamente com não-UTF-8? Precisa?
        csv_file = open(f"/tmp/{tmp_file_name}", "r")
        # Pega primeira linha para detectar separador
        first_line = csv_file.readline()
        csv_file.seek(0)
        sep = detect_separator(first_line)

        if extra_safe:
            # csv_text = fix_csv(csv_text, sep)
            log("[!] 'Fix CSV' for big files is not implemented yet", level="warning")  # FIXME

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
