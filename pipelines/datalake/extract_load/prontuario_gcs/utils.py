# -*- coding: utf-8 -*-
import os
import re
from typing import Dict, List, Tuple
from datetime import datetime
from pipelines.utils.logger import log

##############################################################################################
#                                  EXTRAÇÃO OPENBASE
##############################################################################################


def handle_J(field_bytes, attrs):
    try:
        return int.from_bytes(field_bytes, "big")
    except:
        return None


def handle_U(field_bytes, attrs):
    try:
        return field_bytes.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError:
        return field_bytes.decode("latin-1", errors="replace").strip()


def handle_D(field_bytes, attrs=None):
    b = bytes(field_bytes)
    # formato mais comum que você tem: 2 bytes ano (big-endian) + 5 componentes de 1 byte
    if len(b) >= 7:
        year_be = int.from_bytes(b[0:2], "big")
        month, day, hour, minute, second = b[2], b[3], b[4], b[5], b[6]

        # sanity check simples; se falhar, tente variações
        try:
            return datetime(year_be, month, day, hour, minute, second).isoformat(sep=" ")
        except ValueError:
            # fallback: tente ano little-endian se o layout variar
            year_le = int.from_bytes(b[0:2], "little")
            try:
                return datetime(year_le, month, day, hour, minute, second).isoformat(sep=" ")
            except ValueError:
                # último recurso: devolve hex para debug
                return b.hex()
    else:
        return b.hex()


def handle_others(field_bytes, attrs):
    try:
        return field_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        # print(field_bytes, attrs["type"])
        return field_bytes.strip()


def _find_openbase_folder(data_dir: str) -> str:
    """Localiza a pasta OpenBase no diretório de dados."""
    folders = [name for name in os.listdir(data_dir) if "BASE" in name]
    if not folders:
        raise ValueError(f"Nenhuma pasta OpenBase encontrada em {data_dir}")
    return os.path.join(data_dir, folders[0])


def _get_table_and_dictionary_files(openbase_path: str) -> List[Tuple[str, str]]:
    """Obtém e valida pares de arquivos de tabela e dicionário."""
    tables = sorted([x for x in os.listdir(openbase_path) if x.endswith("._S")])
    dictionaries = sorted([x for x in os.listdir(openbase_path) if x.endswith("._Sd")])
    
    tables_data = list(zip(tables, dictionaries))
    
    # Valida correspondência entre tabelas e dicionários
    for table, dictionary in tables_data:
        if table.replace("._S", "") != dictionary.replace("._Sd", ""):
            log(f"⚠️ Tabela e dicionário não correspondem: {table}, {dictionary}")
    
    return tables_data


def _parse_dictionary_file(dictionary_path: str, encoding: str) -> Dict:
    """Converte arquivo de dicionário em estrutura de dados."""
    with open(dictionary_path, "r", encoding=encoding) as f:
        lines = f.readlines()[3:]  # Ignora as 3 primeiras linhas
    
    structured_dictionary = {}
    acc_offset = 0
    
    for line in lines:
        attrs = line.strip().split()
        name = attrs[0]
        _type = attrs[1].split(",")[0] if "," in attrs[1] else attrs[1]
        size = int(re.sub(r"\D", "", _type))
        
        structured_dictionary[name] = {
            "type": _type,
            "size": size,
            "offset": acc_offset
        }
        acc_offset += size
    
    return structured_dictionary


def _load_all_dictionaries(
    tables_data: List[Tuple[str, str]], 
    openbase_path: str, 
    encoding: str
) -> Dict[str, Dict]:
    """Carrega todos os dicionários de metadados."""
    dictionaries = {}
    
    for table, dictionary in tables_data:
        dictionary_path = os.path.join(openbase_path, dictionary)
        dictionaries[table] = _parse_dictionary_file(dictionary_path, encoding)
    
    return dictionaries


def _get_metadata_info(structured_dictionary: Dict) -> Tuple[List[str], int]:
    """Extrai informações de metadados ordenados e tamanho esperado de registro."""
    metadata = sorted(
        structured_dictionary.keys(),
        key=lambda x: structured_dictionary[x]["offset"]
    )
    last_col = metadata[-1]
    expected_length = (
        structured_dictionary[last_col]["size"] +
        structured_dictionary[last_col]["offset"] + 1
    )
    return metadata, expected_length


def _extract_field_value(field_bytes: bytes, attrs: Dict) -> str:
    """Extrai e converte valor do campo baseado no tipo."""
    field_type = attrs["type"].upper()
    
    if field_type.startswith("J"):
        return handle_J(field_bytes, attrs)
    elif field_type.startswith("U"):
        return handle_U(field_bytes, attrs)
    elif field_type.startswith("D"):
        return handle_D(field_bytes, attrs)
    else:
        return handle_others(field_bytes, attrs)


def _parse_record(rec: bytes, structured_dictionary: Dict) -> Dict:
    """Converte um registro binário em dicionário de valores."""
    row = {}
    
    for col, attrs in structured_dictionary.items():
        field_bytes = rec[attrs["offset"]:attrs["offset"] + attrs["size"]]
        row[col] = _extract_field_value(field_bytes, attrs)
    
    return row


def _write_csv_header(csv_path: str, metadata: List[str]) -> None:
    """Cria arquivo CSV e escreve cabeçalho."""
    with open(csv_path, "w") as f:
        f.write(",".join(metadata) + "\n")


def _write_csv_row(csv_path: str, row: Dict) -> None:
    """Adiciona uma linha ao arquivo CSV."""
    with open(csv_path, "a") as f:
        line = [str(value) for value in row.values()]
        f.write(",".join(line) + "\n")



##############################################################################################
#                                  EXTRAÇÃO POSTGRES
##############################################################################################


def extract_table_name(insert_stmt):
    """Extrai o nome da tabela do comando INSERT INTO."""
    match = re.search(r"INSERT\s+INTO\s+`?([^\s(`]+)`?", insert_stmt, re.IGNORECASE)
    table_name = match.group(1)
    if table_name:
        return table_name.replace("public.", "")
    return None


def extract_columns(insert_stmt):
    """Extrai os nomes das colunas do comando INSERT INTO."""
    match = re.search(
        r"INSERT\s+INTO\s+`?[^\s(`]+`?\s*\(([^)]+)\)\s*VALUES", insert_stmt, re.IGNORECASE
    )
    if match:
        columns = match.group(1).split(",")
        return [col.strip().strip("`") for col in columns]
    return None


def extract_values(insert_stmt):
    """Extrai os valores do comando INSERT INTO, considerando valores complexos."""
    match = re.search(r"VALUES\s*\((.*)\);?\s*$", insert_stmt, re.IGNORECASE | re.DOTALL)
    if not match:
        return None

    values_str = match.group(1)
    values = []
    current_value = ""
    inside_quotes = False
    quote_char = None
    paren_depth = 0
    escape_next = False

    for char in values_str:
        if escape_next:
            current_value += char
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            current_value += char
            continue

        if char in ('"', "'") and (not inside_quotes or char == quote_char):
            inside_quotes = not inside_quotes
            quote_char = char if inside_quotes else None
            current_value += char
        elif char == "(" and not inside_quotes:
            paren_depth += 1
            current_value += char
        elif char == ")" and not inside_quotes:
            paren_depth -= 1
            current_value += char
        elif char == "," and not inside_quotes and paren_depth == 0:
            values.append(current_value.strip())
            current_value = ""
        else:
            current_value += char

    if current_value.strip():
        values.append(current_value.strip())

    return values


def clean_value(value):
    """Remove aspas externas e escapa aspas internas para CSV."""
    value = value.strip()

    # Remove NULL
    if value.upper() == "NULL":
        return ""

    if (value.startswith("'") and value.endswith("'")) or (
        value.startswith('"') and value.endswith('"')
    ):
        value = value[1:-1]

    # Decodifica escapes comuns do SQL
    value = value.replace("\\'", "'")
    value = value.replace('\\"', '"')
    value = value.replace("\\n", "\n")
    value = value.replace("\\r", "\r")
    value = value.replace("\\t", "\t")
    value = value.replace("\\\\", "\\")

    # Escapa aspas duplas para formato CSV
    value = value.replace('"', '""')

    # Se contém vírgula, quebra de linha ou aspas, envolve em aspas
    if "," in value or "\n" in value or '"' in value:
        value = f'"{value}"'

    return value


def process_insert_statement(
    insert_stmt: str,
    output_path: str,
    target_tables: list = [],
):
    """Processa uma única linha de INSERT e:
    - Verifica se é uma comando válido
    - Verifica se o comando faz referência a uma tabela desejada
    - Extrai nome da tabela, colunas e valores do INSERT

    Retorna uma tupla com (nesta ordem) uma flag booleana indicando sucesso, nome do arquivo CSV
    onde foi inserido os valores e o nome da tabela

    Args:
        insert_stmt (_str_): string com o comando insert
        output_path (_str_): Diretório do arquivo do arquivo com as informações extraídas
        target_tables (_list_, optional): Lista com os nomes das tabelas desejadas. Se vazio extrai qualquer
        linha de insert

    Returns:
        _tuple_: Flag indicadora de sucesso, nome do arquivo CSV gerado e nome da tabela extraída.
    """
    insert_stmt = insert_stmt.strip()
    if not insert_stmt:
        return False, "", ""

    # Extrai informações do comando INSERT
    table_name = extract_table_name(insert_stmt)
    if not table_name:
        return False, "", ""

    # Verifica se a tabela está na lista de tabelas desejadas
    if target_tables and table_name not in target_tables:
        return False, "", table_name

    columns = extract_columns(insert_stmt)
    values = extract_values(insert_stmt)

    if not columns or not values:
        return False, "", table_name

    # Define o nome do arquivo CSV
    csv_filename = f"{output_path}/{table_name}.csv"

    if not os.path.exists(csv_filename):
        # Cria novo arquivo e escreve cabeçalho
        with open(csv_filename, "w", encoding="utf-8", newline="") as csv_file:
            csv_file.write(",".join(columns) + "\n")

    # Processa e grava os valores diretamente no arquivo
    with open(csv_filename, "a", encoding="utf-8", newline="") as csv_file:
        cleaned_values = [clean_value(v) for v in values]
        csv_file.write(",".join(cleaned_values) + "\n")

    return True, csv_filename, table_name


def process_sql_file_streaming(input_file, output_path, target_tables=None, buffer_size=65536):
    """Processa o arquivo SQL linha por linha sem carregar tudo na memória.

    Args:
        input_file: Caminho do arquivo SQL
        target_tables: Lista com nomes das tabelas a extrair (None = todas)
        buffer_size: Tamanho do buffer de leitura em bytes
    """

    # Dicionário para rastrear quais tabelas já foram inicializadas
    table_files = {}
    table_columns = {}

    # Converte lista de tabelas para conjunto para busca mais rápida
    if target_tables:
        target_tables = set(target_tables)

    # Buffer para acumular o comando INSERT atual
    current_insert = ""
    line_count = 0
    processed_count = 0

    print("Iniciando processamento...")

    with open(input_file, "r", encoding="utf-8", buffering=buffer_size) as f:
        for line in f:
            line_count += 1

            # Feedback de progresso a cada 100000 linhas
            if line_count % 500_000 == 0:
                print(f"Processadas {line_count} linhas, {processed_count} registros extraídos...")

            # Remove espaços em branco no início e fim
            line = line.strip()

            # Ignora linhas vazias e comentários
            if not line or line.startswith("--") or line.startswith("/*") or line.startswith("#"):
                continue

            # Verifica se é início de um INSERT INTO
            if re.match(r"INSERT\s+INTO", line, re.IGNORECASE):
                # Se já havia um INSERT acumulado, processa ele
                if current_insert:
                    if process_insert_statement(
                        current_insert, table_files, table_columns, output_path, target_tables
                    ):
                        processed_count += 1

                # Inicia novo INSERT
                current_insert = line
            else:
                # Continua acumulando o INSERT atual
                if current_insert:
                    current_insert += " " + line

            # Verifica se o INSERT está completo (termina com ;)
            if current_insert and current_insert.rstrip().endswith(";"):
                if process_insert_statement(
                    current_insert, table_files, table_columns, output_path, target_tables
                ):
                    processed_count += 1
                current_insert = ""

    # Processa o último INSERT se houver
    if current_insert:
        if process_insert_statement(
            current_insert, table_files, table_columns, output_path, target_tables
        ):
            processed_count += 1

    print(f"\nProcessamento concluído!")


if __name__ == "__main__":
    input_file = "data/hospub-2269945-VISUAL-10-11-2025-23h30m/hospub.sql"
    tables_to_extract = [
        "public.hp_rege_evolucao",
        "public.hp_rege_ralta",
        "public.hp_rege_receituario",
        "public.hp_descricao_cirurgia",
        "public.hp_rege_emerg",
        "public.hp_prontuario_be",
    ]
    process_sql_file_streaming(
        input_file=input_file, output_path="upload", target_tables=tables_to_extract
    )
