# -*- coding: utf-8 -*-
import datetime
import io
import re
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

    first_line = filter_bad_chars(csv_text.splitlines()[0])
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
        new_line = filter_bad_chars(line)
        diff = max_cols - (new_line.count(sep) + 1)
        new_line += sep * diff
        other_lines[i] = new_line

    new_first_line = sep.join(columns) + "\n"
    new_csv_text = new_first_line + "\n".join(other_lines)

    return new_csv_text


def fix_csv_file(csv_file, sep: str, detected_encoding: str):
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
    first_line = filter_bad_chars(data.decode(detected_encoding))

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
    # Para cada linha, removemos caracteres inesperados
    # e garantimos que há o mesmo número de valores que de colunas
    for data in csv_file:
        line = data.decode(detected_encoding)
        new_line = filter_bad_chars(line)
        # Padroniza número de campos por linha
        diff = max_cols - (new_line.count(sep) + 1)
        new_line += (sep * diff) + "\n"
        new_csv_file.write(new_line.encode("utf-8"))

    # .close() mata o arquivo temporário inicial
    csv_file.close()
    return new_csv_file


def fix_AP22_LISTAGEM_VACINA_V2_202408(csv_file):
    new_csv_file = tempfile.TemporaryFile()
    csv_file.seek(0)
    data = csv_file.readline()
    first_line = filter_bad_chars(data.decode("cp850")) + "\n"
    new_csv_file.write(first_line.encode("utf-8"))
    for data in csv_file:
        line = data.decode("cp1252")
        new_line = filter_bad_chars(line) + "\n"
        new_csv_file.write(new_line.encode("utf-8"))
    csv_file.close()
    new_csv_file.seek(0)
    return new_csv_file


def fix_INDICADORES_VARIAVEL_3(csv_text, sep):
    import csv

    """
    Ajusta as colunas do arquivo G11 e G12,
    garantindo que tenham exatamente as colunas do schema esperado e presente nos demais grupos.

    Para arquivos cujo nome contenha os padrões "+INDICADORES_VARIAVEL_3_G11_CAP+"
    ou "+INDICADORES_VARIAVEL_3_G12_CAP+", esta função adiciona as colunas faltantes
    com valor vazio e reordena as colunas conforme o esquema esperado.

    Retorna o conteúdo corrigido como string.
    """
    expected_columns = [
        "INDICADOR",
        "AP",
        "CNES",
        "UNIDADE",
        "INE",
        "EQUIPE",
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "TOTAL",
    ]

    input_io = io.StringIO(csv_text)
    reader = csv.DictReader(input_io, delimiter=sep)

    output_io = io.StringIO()
    writer = csv.DictWriter(
        output_io, fieldnames=expected_columns, delimiter=sep, lineterminator="\n"
    )

    writer.writeheader()
    for row in reader:
        new_row = {col: row[col] if col in row else None for col in expected_columns}
        writer.writerow(new_row)

    fixed_text = output_io.getvalue()

    new_csv_file = tempfile.TemporaryFile()
    new_csv_file.write(fixed_text.encode("utf-8"))
    new_csv_file.seek(0)
    return new_csv_file


def shorten_column_name(column_name: str, max_len: int = 30) -> str:
    """
    Gera um nome de coluna compatível com o BigQuery, removendo
    ocorrências indesejadas e normalizando o resultado.
    Em específico no arquivo TEA, podendo ser expandida e melhor refatorada.

    :param column_name: Nome original da coluna.
    :param max_len: Tamanho máximo permitido (default: 30 caracteres).
    :return: Nome de coluna ajustado.
    """
    # 1. Passo inicial: aplica fix_column_name
    sanitized = fix_column_name(column_name)
    # 2. Remove variações "r/f" ou "rf" e "?"
    sanitized = re.sub(r"(?i)r[ _/-]*f", "", sanitized)
    sanitized = sanitized.replace("?", "")
    # 3. Normaliza todos os caracteres que não sejam [a-z0-9] ⇒ "_", e colapsa underscores repetidos
    normalized = re.sub(r"[^a-z0-9]+", "_", sanitized.lower()).strip("_")
    normalized = re.sub(r"_+", "_", normalized)

    # Se já cabe em max_len, devolve:
    if len(normalized) <= max_len:
        return normalized

    # 4. Caso ultrapasse max_len, tokeniza e trunca:
    stopwords = {
        "a",
        "o",
        "os",
        "as",
        "de",
        "do",
        "da",
        "dos",
        "das",
        "quando",
        "vc",
        "com",
        "ou",
        "para",
        "seu",
        "sua",
        "isso",
        "que",
        "é",
    }
    tokens = normalized.split("_")
    prefix_set = {"mchat", "aq10"}
    selected = []

    # Preserva prefixo importante
    if tokens and tokens[0] in prefix_set:
        selected.append(tokens.pop(0))

    # Filtra stopwords e tokens vazios
    tokens = [t for t in tokens if t and t not in stopwords and t not in {"r", "rf"}]

    for tok in tokens:
        candidate = "_".join(selected + [tok])
        if len(candidate) <= max_len:
            selected.append(tok)
        else:
            available = max_len - (len("_".join(selected)) + 1)  # 1 para o underscore
            if available > 3:
                selected.append(tok[:available])
            break

    short_name = "_".join(selected)[:max_len]
    short_name = re.sub(r"_+", "_", short_name).strip("_")
    return short_name


def fix_LISTAGEM_ATENDIMENTOS_PACIENTES_TEA(csv_file):
    """
    Processa o CSV de atendimentos TEA, aplicando a sanitização e encurtamento dos nomes de coluna.
    """
    new_csv_file = tempfile.TemporaryFile()
    csv_file.seek(0)

    header_data = csv_file.readline()
    if isinstance(header_data, bytes):
        try:
            header_line = header_data.decode("cp1252").strip()
        except UnicodeDecodeError:
            header_line = header_data.decode("utf-8", errors="replace").strip()
    else:
        header_line = header_data.strip()

    sep = detect_separator(header_line)
    original_columns = header_line.split(sep)

    new_columns = []
    seen = {}
    for col in original_columns:
        sanitized = fix_column_name(col)
        normalized = re.sub(r"[^a-z0-9]+", "_", sanitized.lower()).strip("_")
        normalized = re.sub(r"_+", "_", normalized)

        if len(normalized) > 30:
            new_col = shorten_column_name(col, max_len=30)
        else:
            new_col = normalized

        base = new_col
        if base in seen:
            seen[base] += 1
            new_col = f"{base}_{seen[base]}"
        else:
            seen[base] = 1

        new_columns.append(new_col)
        # log(f"[DEBUG] original: {col}\nnovo: {new_col}")

    new_header = sep.join(new_columns) + "\n"
    new_csv_file.write(new_header.encode("utf-8"))

    rest = csv_file.read()
    if isinstance(rest, bytes):
        new_csv_file.write(rest)
    else:
        new_csv_file.write(rest.encode("utf-8"))

    csv_file.close()
    new_csv_file.seek(0)
    return new_csv_file


def filter_bad_chars(row: str) -> str:
    EMPTY = ""
    SPACE = " "
    replace_pairs = [
        ("\u00ad", EMPTY),  # Soft Hyphen
        ("\u200c", EMPTY),  # Zero Width Non-Joiner
        ("\t", SPACE),  # Tab
        ("\n", SPACE),  # Line feed
        ("\u00a0", SPACE),  # No-Break Space
    ]
    for pair in replace_pairs:
        row = row.replace(pair[0], pair[1])

    # NULL, \r, etc
    row = re.sub(r"[\u0000-\u001F]", EMPTY, row)
    # DEL, outros de controle
    row = re.sub(r"[\u007F-\u009F]", EMPTY, row)
    # Espaços de tamanhos diferentes
    row = re.sub(r"[\u2000-\u200B\u202F\u205F]", SPACE, row)
    # LTR/RTL marks, overrides
    row = re.sub(r"[\u200E-\u202E]", EMPTY, row)

    return row.strip()


def fix_column_name(column_name: str) -> str:
    column_name = filter_bad_chars(column_name)

    replace_from = [None] * 2
    replace_to = [None] * 2

    replace_from[0] = ["(", ")", "\r", "\n"]
    replace_to[0] = ""

    replace_from[1] = [" ", "[", "]", "<", ">", "-", ".", ",", ";", "/", "\\", "'", '"']
    replace_to[1] = "_"

    column_name = unidecode(column_name).lower()
    for from_list, to_char in zip(replace_from, replace_to):
        for from_char in from_list:
            column_name = column_name.replace(from_char, to_char)

    return column_name


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

        if file_name.endswith("AP22_LISTAGEM_VACINA_V2_2024-08.csv"):
            # Arquivo extremamente bizarro: a primeira linha é codificada em CP-850, e
            # o resto em CP-1252 (????). Será que é só esse? Zero sentido. --Avellar
            csv_file = fix_AP22_LISTAGEM_VACINA_V2_202408(csv_file)

        # Pega primeira linha para detectar separador
        data = csv_file.readline()
        detected_encoding = chardet.detect(data)["encoding"]
        first_line = data.decode(detected_encoding)
        log(f"[download_file] Detected encoding of '{detected_encoding}'")

        if len(first_line) <= 0:
            log("[download_file] First line is empty; error likely", level="warning")
        sep = detect_separator(first_line)
        log(f"[download_file] Detected separator: '{sep}'")

        csv_file = fix_csv_file(csv_file, sep, detected_encoding)
        csv_file.seek(0)

    # Caso o arquivo seja pequeno o suficiente
    else:
        # Download do arquivo em memória
        data = blob.download_as_bytes()
        detected_encoding = chardet.detect(data)["encoding"]
        csv_text = data.decode(detected_encoding)
        sep = detect_separator(csv_text)

        if (
            "INDICADORES_VARIAVEL_3_G11_CAP" in file_name
            or "INDICADORES_VARIAVEL_3_G12_CAP" in file_name
        ):
            csv_file = fix_INDICADORES_VARIAVEL_3(csv_text, sep)
        elif "LISTAGEM_ATENDIMENTOS_PACIENTES_TEA" in file_name:
            csv_file = fix_LISTAGEM_ATENDIMENTOS_PACIENTES_TEA(io.StringIO(csv_text))
        else:
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
