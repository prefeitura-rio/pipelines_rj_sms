# -*- coding: utf-8 -*-
"""
Tarefas
"""

from datetime import datetime, timedelta

# Geral
import mysql.connector
import pandas as pd
from mysql.connector import Error

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def connect_mysql(host: str, database: str, user: str, password: str, port: str = None):
    """
    Esta tarefa estabelece uma conexão com o banco MySQL usando os parâmetros fornecidos.

    Parâmetros:
        host (str): Endereço do servidor MySQL.
        database (str): Nome do banco de dados para conectar.
        user (str): Usuário do banco de dados.
        password (str): Senha do banco de dados.
        port (str, opcional): Porta do servidor MySQL. Padrão é None.

    Retorna:
        connection (mysql.connector.connection.MySQLConnection):
        Objeto de conexão estabelecida.
    """
    log(f"Conectando ao MySQL com os parâmetros: host={host}, database={database}, port={port}")

    try:
        if port:
            connection = mysql.connector.connect(
                host=host, database=database, user=user, password=password, port=port
            )
        else:
            connection = mysql.connector.connect(
                host=host, database=database, user=user, password=password
            )

        if connection.is_connected():
            log("Conexão com MySQL estabelecida com sucesso.")
            return connection
        else:
            raise Exception("Falha ao estabelecer conexão com o MySQL.")

    except Error as e:
        log(f"Erro ao tentar conectar ao MySQL: {e}")
        raise


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def query_mysql(connection, query: str):
    """
    Esta tarefa recebe uma conexão MySQL ativa e uma query SQL,
    executando a consulta e retornando um DataFrame com os resultados.

    Parâmetros:
        connection (mysql.connector.connection.MySQLConnection):
        Objeto de conexão ativa com o MySQL.
        query (str): Comando SQL a ser executado.

    Retorna:
        df (pd.DataFrame): DataFrame com os resultados da consulta.
    """
    # Registrando parâmetros em português
    log(f"Executando query no MySQL: {query}")

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        df = pd.DataFrame(records)

        log(f"Número total de linhas retornadas pela consulta: {cursor.rowcount}")
        cursor.close()

        return df

    except Error as e:
        log(f"Erro ao executar a query no MySQL: {e}")
        raise


@task
def get_col_names(connection, df, table):
    """
    Esta tarefa recebe uma conexão MySQL ativa, um DataFrame e o nome da tabela,
    retornando uma lista com os nomes das colunas da tabela.

    Parâmetros:
        connection (mysql.connector.connection.MySQLConnection):
        Objeto de conexão ativa com o MySQL.
        df (pd.DataFrame): DataFrame com os resultados da consulta.
        table (str): Nome da tabela do banco de dados.

    Retorna:
        col_names (list): Lista com os nomes das colunas da tabela.
    """
    # Registrando parâmetros em português
    log(f"Iniciando a tarefa para obter nomes das colunas da tabela: {table}")

    try:
        cursor = connection.cursor()
        cursor.execute(f"show columns from {table}")
        records = cursor.fetchall()
        col_names = [record[0] for record in records]

        cursor.close()

        df.columns = col_names

        log(f"Nomes das colunas obtidos com sucesso para a tabela: {table}")

        df["data_extracao"] = datetime.now()

        return df

    except Error as e:
        log(f"Erro ao obter os nomes das colunas da tabela: {e}")
        raise


@task
def close_mysql(connection):
    """
    Esta tarefa fecha a conexão com o banco MySQL, caso esteja aberta.

    Parâmetros:
        connection (mysql.connector.connection.MySQLConnection):
        Objeto de conexão ativa com o MySQL.

    Retorna:
        Nenhum.
    """

    try:
        if connection.is_connected():
            connection.close()
            log("Conexão com MySQL foi fechada com sucesso.")
    except Error as e:
        log(f"Erro ao tentar fechar a conexão com o MySQL: {e}")
        raise


@task
def query_mysql_all_in_one(
    host: str, database: str, user: str, password: str, query: str, table: str, port: str = None
):
    """
    Esta tarefa conecta ao banco MySQL, executa uma consulta SQL, obtém os nomes das colunas
    e retorna os resultados em um DataFrame.

    Parâmetros:
        host (str): Endereço do servidor MySQL.
        database (str): Nome do banco de dados para conectar.
        user (str): Usuário do banco de dados.
        password (str): Senha do banco de dados.
        query (str): Comando SQL a ser executado.
        table (str): Nome da tabela do banco de dados para obter os nomes das colunas.
        port (str, opcional): Porta do servidor MySQL. Padrão é None.

    Retorna:
        df (pd.DataFrame): DataFrame com os resultados da consulta.
    """
    log(f"Conectando ao MySQL com os parâmetros: host={host}, database={database}, port={port}")

    connection = None
    try:
        # Estabelecendo conexão com o MySQL
        connection = (
            mysql.connector.connect(
                host=host, database=database, user=user, password=password, port=port
            )
            if port
            else mysql.connector.connect(host=host, database=database, user=user, password=password)
        )

        if connection.is_connected():
            log("Conexão com MySQL estabelecida com sucesso.")

            # Executando a consulta SQL
            log(f"Executando query no MySQL: {query}")
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            df = pd.DataFrame(records)
            log(f"Número total de linhas retornadas pela consulta: {cursor.rowcount}")
            cursor.close()

            # Obtendo os nomes das colunas
            log(f"Iniciando a tarefa para obter nomes das colunas da tabela: {table}")
            cursor = connection.cursor()
            cursor.execute(f"show columns from {table}")
            records = cursor.fetchall()
            col_names = [record[0] for record in records]
            cursor.close()

            df.columns = col_names
            log(f"Nomes das colunas obtidos com sucesso para a tabela: {table}")

            df["data_extracao"] = datetime.now()

            return df
        else:
            raise Exception("Falha ao estabelecer conexão com o MySQL.")

    except Error as e:
        log(f"Erro ao tentar conectar ao MySQL ou executar a query: {e}")
        raise

    finally:
        # Fechando a conexão com o MySQL
        if connection and connection.is_connected():
            try:
                connection.close()
                log("Conexão com MySQL foi fechada com sucesso.")
            except Error as e:
                log(f"Erro ao tentar fechar a conexão com o MySQL: {e}")
                raise
