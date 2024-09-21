# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Tasks for DataSUS pipelines
"""

from datetime import datetime, date
import calendar
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log


from pipelines.utils.credential_injector import authenticated_task as task

from pipelines.reports.farmacia_digital.livro_controlados.livro_controlados.extracao import (
    dados_com_movimentacao,
    dados_sem_movimentacao,
    livros_para_gerar,
)
from pipelines.reports.farmacia_digital.livro_controlados.livro_controlados.relatorio import (
    gerar_relatorios,
)

from pipelines.datalake.utils.data_extraction.google_drive import upload_folder_to_gdrive


def get_month_range(date_str: str) -> tuple[str, str]:
    """
    Generate the first and last day of the month based on a date string.

    Args:
        date_str (str): Date string in the format "YYYY-MM"

    Returns:
        tuple[str, str]: First day and last day of the month in "DD.MM.YY" format
    """
    date_obj = datetime.strptime(date_str, "%Y-%m")
    first_day = date(date_obj.year, date_obj.month, 1)
    _, last_day = calendar.monthrange(date_obj.year, date_obj.month)
    last_date = date(date_obj.year, date_obj.month, last_day)

    return (first_day.strftime("%d.%m.%y"), last_date.strftime("%d.%m.%y"))


@task
def generate_report(output_directory: str, competencia: str):
    """
    Generate a report
    """
    # log.info("Extracting data with movement records")
    dataset_com_movimentacao = dados_com_movimentacao()
    dataset_sem_movimentacao = dados_sem_movimentacao()
    livros = livros_para_gerar()

    competencia_inicio, competencia_fim = get_month_range(competencia)

    template = "pipelines/reports/farmacia_digital/livro_controlados/template/template.docx"

    gerar_relatorios(
        df_relacao=livros,
        df_com_mov=dataset_com_movimentacao,
        df_sem_mov=dataset_sem_movimentacao,
        base_directory=output_directory,
        competencia_inicio=competencia_inicio,
        competencia_fim=competencia_fim,
        competencia=competencia,
        template=template,
    )

@task
def upload_report_to_gdrive(folder_path: str, folder_id: str):
    """
    Upload a report to Google Drive
    """
    
    upload_folder_to_gdrive(folder_path, folder_id)