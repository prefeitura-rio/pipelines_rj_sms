# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Extraction functions for Farmácia Digital - Livro de Medicamentos Controlados
"""
import pandas as pd
from google.cloud import bigquery


def dados_com_movimentacao() -> pd.DataFrame:

    client = bigquery.Client()

    query = """
    SELECT * FROM rj-sms-dev.thiago__projeto_estoque.report_medicamentos_controlados__itens_com_movimento
    """

    query_job = client.query(query)

    results = query_job.result()
    df = results.to_dataframe(dtypes={"id_cnes": "str", "id_material": "str"})

    df.sort_values(by=["id_cnes", "nome", "ordem"], inplace=True)
    df.movimento_quantidade = df.movimento_quantidade.astype(int)
    df.posicao_final = df.posicao_final.astype(int)

    return df


def dados_sem_movimentacao() -> pd.DataFrame:

    client = bigquery.Client()

    query = """
    SELECT * FROM rj-sms-dev.thiago__projeto_estoque.report_medicamentos_controlados__itens_sem_movimento
    """

    query_job = client.query(query)
    results = query_job.result()
    df = results.to_dataframe(dtypes={"id_cnes": "str", "id_material": "str"})

    df.posicao_atual = df.posicao_atual.astype(int)

    return df


def livros_para_gerar() -> pd.DataFrame:
    client = bigquery.Client()

    query = """
    SELECT * FROM rj-sms-dev.thiago__projeto_estoque.report_medicamentos_controlados__relacao_relatorios
    """
    query_job = client.query(query)
    results = query_job.result()

    return results.to_dataframe(dtypes={"id_cnes": "str"})
