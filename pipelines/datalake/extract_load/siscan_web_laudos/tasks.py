# -*- coding: utf-8 -*-
"""
Tarefas
"""

# Geral
import os
from datetime import datetime, timedelta

import pandas as pd

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.siscan_web_laudos.scraper import run_scraper
from pipelines.datalake.utils.tasks import delete_file
from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def run_siscan_scraper(
    email: str,
    password: str,
    opcao_exame: str,
    start_date: str,
    end_date: str,
    output_dir: str = ".",
):
    """
    Executa o scraper do SISCAN para coletar dados de pacientes em um intervalo de datas.

    Parâmetros:
        email (str): E-mail de acesso ao sistema SISCAN.
        password (str): Senha de acesso ao sistema SISCAN.
        start_date (str): Data inicial no formato 'dd/mm/YYYY'.
        end_date (str): Data final no formato 'dd/mm/YYYY'.
        output_dir (str): Diretório onde os arquivos parquet serão salvos.

    Retorna:
        List[str]: Lista de caminhos dos arquivos parquet gerados.
    """

    try:
        log(f"Iniciando tarefa de coleta de dados do SISCAN")
        log(f"Período: {start_date} a {end_date}")
        log(f"Tipo de exame: {opcao_exame}")
        log(f"Diretório de saída: {output_dir}")

        pacientes = run_scraper(
            email=email,
            password=password,
            opcao_exame=opcao_exame,
            start_date=start_date,
            end_date=end_date,
            headless=True,
        )
        log(f"Dados coletados com sucesso. Total de registros: {len(pacientes)}")

        df = pd.DataFrame(pacientes)
        log(f"DataFrame criado com {len(df)} linhas")

        begin = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y%m%d")
        end_ = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y%m%d")

        filename = f"siscan_extraction_{begin}_{end_}.parquet"
        filepath = os.path.join(output_dir, filename)

        log(f"Salvando arquivo: {filename}")
        df.to_parquet(filepath, index=False)

        log(f"Arquivo salvo com sucesso: {filepath}")
        log(f"Registros processados: {len(df)}")
        log(f"Coleta concluída para o período de {start_date} a {end_date}")

        return filepath

    except Exception as e:
        log(f"Erro durante a execução do scraper: {e}")
        raise


@task
def check_records(file_path: str) -> bool:
    """Checa se há registros no arquivo parquet.


    Args:
        file_path (str): caminho do arquivo

    Returns:
        bool: indicador se há ou não registros.
    """
    df = pd.read_parquet(file_path)

    if df.empty:
        log("Não há registros para processar.")
        delete_file.run(file_path=file_path)
        log("O arquivo vazio foi excluído.")
        return False

    return True


@task
def generate_extraction_windows(start_date: datetime, end_date: datetime, interval: int) -> list:
    """Gera janelas de extração de dados com base em duas datas e um intervalo de dias.

    Args:
        start_date (datetime): Data inicial
        end_date (datetime): Data final
        interval (int): Número de dias por janela

    Returns:
        List[tuple]: Lista de tuplas com as datas de início e fim de cada janela
    """
    intervals = []
    if not end_date:
        end_date = datetime.now()
    while start_date <= end_date:
        end_interval = start_date + timedelta(days=interval - 1)
        if end_interval > end_date:
            end_interval = end_date
        intervals.append((start_date.strftime("%d/%m/%Y"), end_interval.strftime("%d/%m/%Y")))
        start_date = end_interval + timedelta(days=1)

    return intervals


@task
def build_operator_parameters(
    start_dates: tuple,
    end_dates: tuple,
    bq_table: str,
    bq_dataset: str,
    opcao_exame: str,
    environment: str = "dev",
) -> list:
    """Gera lista de parâmetros para o(s) operator(s).

    Args:
        start_dates (tuple): Data inicial da extração
        end_dates (tuple): Data final da extração
        environment (str, optional): Ambiente de execução. Defaults to 'dev'.

    Returns:
        List[dict]: Lista com os parâmetros do(s) operator(s).
    """
    return [
        {
            "environment": environment,
            "data_inicial": start,
            "data_final": end,
            "bq_dataset": bq_dataset,
            "bq_table": bq_table,
            "opcao_exame": opcao_exame,
        }
        for start, end in zip(start_dates, end_dates)
    ]


@task
def parse_date(date: str) -> datetime:
    return datetime.strptime(date, "%d/%m/%Y")
