# -*- coding: utf-8 -*-
"""
Tarefas
"""

# Geral
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

# Internas
from prefeitura_rio.pipelines_utils.logging import log
from pymongo import MongoClient
from pymongo.database import Database

from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=5, retry_delay=timedelta(minutes=3), nout=2)
def create_mongodb_connection(
    host: str, port: int, user: str, password: str, authsource: str, db_name: str
) -> Tuple[MongoClient, Database]:
    """
    Cria uma conexão com o MongoDB e retorna tanto o cliente quanto o objeto de banco de dados.

    Parâmetros
    ----------
    host : str
        Endereço (host) do servidor MongoDB.
    port : int
        Porta do servidor MongoDB.
    user : str
        Usuário de autenticação no MongoDB.
    password : str
        Senha de autenticação no MongoDB.
    authsource : str
        Base de autenticação do MongoDB (geralmente o próprio nome do banco ou 'admin').
    db_name : str
        Nome do banco de dados ao qual queremos nos conectar.

    Retorna
    -------
    Tuple[MongoClient, Database]
        Retorna uma tupla contendo o cliente (MongoClient) e o banco de dados (Database).
    """

    log("Iniciando criação de conexão com o MongoDB...")
    log(f"Parâmetros recebidos: host={host}, port={port}, user={user}, db_name={db_name}")

    # Monta a string de conexão de forma genérica
    # Exemplo: "mongodb://usuario:senha@host:porta/?authSource=db_auth"
    connection_string = f"mongodb://{user}:{password}@{host}:{port}/?authSource={authsource}"

    # Cria o cliente
    client = MongoClient(connection_string)

    # Obtem o objeto de banco de dados
    db = client[db_name]

    log("Conexão com MongoDB criada com sucesso.")
    return client, db


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def get_collection_data(
    db: Database,
    collection_name: str,
    query: Dict[str, Any] = None,
    sample_size: int = 0,
) -> List[Dict[str, Any]]:
    """
    Retorna dados de uma coleção (tabela) MongoDB, dado um filtro (query), projeção e opcionalmente
    um limite.

    Parâmetros
    ----------
    db : Database
        Objeto de banco de dados (Database) já conectado ao MongoDB.
    collection_name : str
        Nome da coleção (tabela) a ser consultada.
    query : Dict[str, Any], opcional
        Filtro de pesquisa no formato de dicionário do MongoDB (default=None, que significa buscar
          tudo).
    sample_size : int, opcional
        Limite de documentos a serem retornados. 0 é o equivalente a buscar tudo (default=0).

    Retorna
    -------
    List[Dict[str, Any]]
        Retorna uma lista de dicionários, onde cada dicionário representa um documento.
    """

    log(f"Iniciando busca de dados na coleção '{collection_name}'.")
    log(f"Parâmetros: query={query}, sample_size={sample_size}")

    if query is None:
        query = {}

    collection = db[collection_name]

    # Executa a busca no MongoDB
    cursor = collection.find(filter=query, limit=sample_size)

    # Converte o cursor em uma lista de dicionários
    data = list(cursor)

    log(f"Busca finalizada. Foram retornados {len(data)} documentos.")
    return data


@task
def to_pandas(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Converte uma lista de dicionários (documentos) em um DataFrame pandas.

    Parâmetros
    ----------
    data : List[Dict[str, Any]]
        Lista de documentos (dicionários) retornados de uma consulta no MongoDB.

    Retorna
    -------
    pd.DataFrame
        DataFrame contendo os dados.
    """

    log("Iniciando conversão dos dados em pandas DataFrame...")

    # Use json_normalize para expandir colunas aninhadas
    df = pd.json_normalize(data)

    # Se existir a coluna '_id', converta para string (ou remova, conforme necessidade)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    df["data_extracao"] = datetime.now()

    log(
        f"Conversão concluída. O DataFrame resultante tem {df.shape[0]} linhas \
        e {df.shape[1]} colunas."
    )

    return df


@task
def close_mongodb_connection(client: MongoClient) -> None:
    """
    Encerra a conexão com o MongoDB.

    Parâmetros
    ----------
    client : MongoClient
        Objeto cliente do MongoDB a ser fechado.

    Retorna
    -------
    None
    """

    log("Iniciando fechamento da conexão com o MongoDB...")
    client.close()
    log("Conexão com o MongoDB encerrada com sucesso.")


##################################################################################################
# OPCIONAL: Tarefas adicionais para inspeção de coleções e campos
@task
def list_collections(db: Database) -> List[str]:
    """
    Lista todas as coleções (tabelas) disponíveis em um banco de dados MongoDB.

    Parâmetros
    ----------
    db : Database
        Objeto de banco de dados (Database) já conectado ao MongoDB.

    Retorna
    -------
    List[str]
        Retorna a lista de nomes de coleções (tabelas).
    """

    log("Iniciando listagem de coleções (tabelas) no banco de dados...")
    collections = db.list_collection_names()
    log(f"Foram encontradas {len(collections)} coleções no banco de dados.")
    return collections


@task
def list_columns(db: Database, collection_name: str, sample_size: int = 1) -> Set[str]:
    """
    Lista colunas (campos) em uma coleção (tabela) do MongoDB,
    baseado em uma amostra de documentos.

    Observação: MongoDB é um banco NoSQL sem esquema fixo. Logo,
    as 'colunas' podem variar de documento para documento.

    Parâmetros
    ----------
    db : Database
        Objeto de banco de dados (Database) já conectado ao MongoDB.
    collection_name : str
        Nome da coleção (tabela) a ser inspecionada.
    sample_size : int, opcional
        Número de documentos para analisar a fim de extrair as colunas (default=1).

    Retorna
    -------
    Set[str]
        Retorna um conjunto com os nomes das colunas encontradas.
    """

    log(f"Iniciando listagem de colunas na coleção '{collection_name}'.")
    log(f"Parâmetros: collection_name={collection_name}, sample_size={sample_size}")

    collection = db[collection_name]
    columns_set = set()

    # Realiza a busca nos documentos, limitando a sample_size.
    # Caso haja poucos documentos, ele retornará a quantidade disponível.
    cursor = collection.find({}, limit=sample_size)

    for doc in cursor:
        # Pegamos as chaves do dicionário (documento) e adicionamos a um set
        columns_set.update(doc.keys())

    log(f"Colunas encontradas na coleção '{collection_name}': {columns_set}")
    return columns_set
