import csv
import re
import os
import pandas as pd
from pathlib import Path
from pprint import pprint # Debugging
from datetime import datetime
from google.cloud import storage
from google.api_core import exceptions

def handle_J(field_bytes, attrs):
    try:
        return int.from_bytes(field_bytes, "big")
    except:
        return None

def handle_U(field_bytes, attrs):
    try:
        return field_bytes.decode('utf-8', errors="strict").strip()
    except UnicodeDecodeError:
        return field_bytes.decode('latin-1', errors="replace").strip()

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
        return field_bytes.decode('utf-8', errors="strict")
    except UnicodeDecodeError:
        #print(field_bytes, attrs["type"])
        return field_bytes.strip()
    
def read_table(table_name, structured_dictionary):
    with open(table_name, "rb") as f:
        rows = []

        metadata = list(structured_dictionary.keys())
        metadata = sorted(metadata, key=lambda x: structured_dictionary[x]["offset"])
        last_col = metadata[-1]
        expected_length = structured_dictionary[last_col]["size"] + structured_dictionary[last_col]["offset"] + 1

        while True:
            rec = f.read(expected_length)
            if not rec:
                break

            row = {}
            for col, attrs in structured_dictionary.items():
                field_bytes = rec[attrs["offset"]:attrs["offset"]+attrs["size"]]#.rstrip(b" \x00")

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
            rows.append(row)

    return pd.DataFrame(rows)

def extract_table_name(insert_stmt):
    """Extrai o nome da tabela do comando INSERT INTO."""
    match = re.search(r'INSERT\s+INTO\s+`?([^\s(`]+)`?', insert_stmt, re.IGNORECASE)
    table_name = match.group(1)
    if table_name:
        return table_name.replace('public.','')        
    return None

def extract_columns(insert_stmt):
    """Extrai os nomes das colunas do comando INSERT INTO."""
    match = re.search(r'INSERT\s+INTO\s+`?[^\s(`]+`?\s*\(([^)]+)\)\s*VALUES', insert_stmt, re.IGNORECASE)
    if match:
        columns = match.group(1).split(',')
        return [col.strip().strip('`') for col in columns]
    return None

def extract_values(insert_stmt):
    """Extrai os valores do comando INSERT INTO, considerando valores complexos."""
    match = re.search(r'VALUES\s*\((.*)\);?\s*$', insert_stmt, re.IGNORECASE | re.DOTALL)
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
            
        if char == '\\':
            escape_next = True
            current_value += char
            continue
            
        if char in ('"', "'") and (not inside_quotes or char == quote_char):
            inside_quotes = not inside_quotes
            quote_char = char if inside_quotes else None
            current_value += char
        elif char == '(' and not inside_quotes:
            paren_depth += 1
            current_value += char
        elif char == ')' and not inside_quotes:
            paren_depth -= 1
            current_value += char
        elif char == ',' and not inside_quotes and paren_depth == 0:
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
    if value.upper() == 'NULL':
        return ''
    
    if (value.startswith("'") and value.endswith("'")) or \
       (value.startswith('"') and value.endswith('"')):
        value = value[1:-1]
    
    # Decodifica escapes comuns do SQL
    value = value.replace("\\'", "'")
    value = value.replace('\\"', '"')
    value = value.replace('\\n', '\n')
    value = value.replace('\\r', '\r')
    value = value.replace('\\t', '\t')
    value = value.replace('\\\\', '\\')
    
    # Escapa aspas duplas para formato CSV
    value = value.replace('"', '""')
    
    # Se contém vírgula, quebra de linha ou aspas, envolve em aspas
    if ',' in value or '\n' in value or '"' in value:
        value = f'"{value}"'
    
    return value

def process_insert_statement(insert_stmt, table_files, table_columns, output_path, target_tables=None):
    """Processa um único comando INSERT e grava no arquivo CSV."""
    insert_stmt = insert_stmt.strip()
    if not insert_stmt:
        return False
    
    # Extrai informações do comando INSERT
    table_name = extract_table_name(insert_stmt)
    if not table_name:
        return False
    
    # Verifica se a tabela está na lista de tabelas desejadas
    if target_tables and table_name not in target_tables:
        return False
    
    columns = extract_columns(insert_stmt)
    values = extract_values(insert_stmt)
    
    if not columns or not values:
        return False
    
    # Define o nome do arquivo CSV
    csv_filename = f"{output_path}/{table_name}.csv"
    
    # Verifica se é a primeira vez que processa esta tabela
    if table_name not in table_files:
        # Cria novo arquivo e escreve cabeçalho
        with open(csv_filename, 'w', encoding='utf-8', newline='') as csv_file:
            csv_file.write(','.join(columns) + '\n')
        table_files[table_name] = True
        table_columns[table_name] = columns
        print(f"Criando arquivo: {csv_filename}")
    
    # Processa e grava os valores diretamente no arquivo
    with open(csv_filename, 'a', encoding='utf-8', newline='') as csv_file:
        cleaned_values = [clean_value(v) for v in values]
        csv_file.write(','.join(cleaned_values) + '\n')
    
    return True

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
    
    with open(input_file, 'r', encoding='utf-8', buffering=buffer_size) as f:
        for line in f:
            line_count += 1
            
            # Feedback de progresso a cada 100000 linhas
            if line_count % 500_000 == 0:
                print(f"Processadas {line_count} linhas, {processed_count} registros extraídos...")
            
            # Remove espaços em branco no início e fim
            line = line.strip()
            
            # Ignora linhas vazias e comentários
            if not line or line.startswith('--') or line.startswith('/*') or line.startswith('#'):
                continue
            
            # Verifica se é início de um INSERT INTO
            if re.match(r'INSERT\s+INTO', line, re.IGNORECASE):
                # Se já havia um INSERT acumulado, processa ele
                if current_insert:
                    if process_insert_statement(current_insert, table_files, table_columns, output_path,target_tables):
                        processed_count += 1
                
                # Inicia novo INSERT
                current_insert = line
            else:
                # Continua acumulando o INSERT atual
                if current_insert:
                    current_insert += " " + line
            
            # Verifica se o INSERT está completo (termina com ;)
            if current_insert and current_insert.rstrip().endswith(';'):
                if process_insert_statement(current_insert, table_files, table_columns, output_path, target_tables):
                    processed_count += 1
                current_insert = ""
    
    # Processa o último INSERT se houver
    if current_insert:
        if process_insert_statement(current_insert, table_files, table_columns, output_path, target_tables):
            processed_count += 1
    
    print(f"\nProcessamento concluído!")
    print(f"Total de linhas lidas: {line_count}")
    print(f"Total de registros extraídos: {processed_count}")
    print(f"\nArquivos CSV gerados:")


if __name__ == '__main__':
    input_file = 'data/hospub-2269945-VISUAL-10-11-2025-23h30m/hospub.sql'
    tables_to_extract = ['public.hp_rege_evolucao', 'public.hp_rege_ralta', 'public.hp_rege_receituario', 'public.hp_descricao_cirurgia',
                                'public.hp_rege_emerg', 'public.hp_prontuario_be']
    process_sql_file_streaming(input_file=input_file,
                               output_path='upload',
                               target_tables=tables_to_extract)
    print('FIM')

