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
from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def run_siscan_scraper(
    email: str, password: str, start_date: str, end_date: str, output_dir: str = "."
):
    """
    Executa o scraper do SISCaN para coletar dados de pacientes em um intervalo de datas.

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
        log(f"Iniciando coleta de dados do SISCaN de {start_date} a {end_date}.")

        pacientes = run_scraper(
            email=email, password=password, start_date=start_date, end_date=end_date, headless=True
        )
        df = pd.DataFrame(pacientes)

        begin = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y%m%d")
        end_ = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y%m%d")

        filename = f"siscan_extraction_{begin}_{end_}.parquet"
        filepath = os.path.join(output_dir, filename)
        df.to_parquet(filepath, index=False)

        log(f"Arquivo salvo: {filepath} ({len(df)} registros)")
        log("Coleta concluída de {start_date} a {end_date}.")

        return filepath

    except Exception as e:
        log(f"Erro durante a execução do scraper: {e}", level="error")
        raise
