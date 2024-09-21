# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Extraction functions for Farmácia Digital - Livro de Medicamentos Controlados
"""
import pandas as pd
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader


def import_sql_with_date(file_path: str, data_inicio: str, data_fim: str) -> str:
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader("."))

    # Load the template
    template = env.get_template(file_path)

    # Render the template with the provided date
    rendered_sql = template.render(data_inicio=data_inicio, data_fim=data_fim)

    return rendered_sql


def dados_com_movimentacao(data_inicio: str, data_fim: str) -> pd.DataFrame:

    client = bigquery.Client()

    query = import_sql_with_date(
        "pipelines/reports/farmacia_digital/livro_controlados/livro_controlados/sql/itens_com_movimento.sql.jinja2",
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    query_job = client.query(query)

    results = query_job.result()
    df = results.to_dataframe(dtypes={"id_cnes": "str", "id_material": "str"})

    df.sort_values(by=["id_cnes", "nome", "ordem"], inplace=True)
    df.movimento_quantidade = df.movimento_quantidade.astype(int)
    df.posicao_final = df.posicao_final.astype(int)

    return df


def dados_sem_movimentacao(data_inicio: str, data_fim: str) -> pd.DataFrame:

    client = bigquery.Client()

    query = import_sql_with_date(
        "pipelines/reports/farmacia_digital/livro_controlados/livro_controlados/sql/itens_sem_movimento.sql.jinja2",
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

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
