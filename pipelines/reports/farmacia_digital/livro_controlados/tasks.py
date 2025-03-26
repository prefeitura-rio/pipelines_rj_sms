# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101, E1121
# flake8: noqa: E501
"""
Tasks for DataSUS pipelines
"""

import calendar
from datetime import date, datetime

from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.reports.farmacia_digital.livro_controlados.constants import (
    constants as report_constants,
)
from pipelines.reports.farmacia_digital.livro_controlados.livro_controlados.extracao import (
    dados_com_movimentacao,
    dados_sem_movimentacao,
    livros_para_gerar,
)
from pipelines.reports.farmacia_digital.livro_controlados.livro_controlados.relatorio import (
    gerar_relatorios,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.google_drive import upload_folder_to_gdrive


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

    return (first_day, last_date)


@task
def get_google_drive_folder_id(environment: str) -> str:
    """
    Get the Google Drive folder ID based on the environment
    """
    return report_constants.GOOGLE_DRIVE_FOLDER_ID.value[environment]


@task
def get_reference_date(competencia: str) -> str:
    if competencia == "atual":
        return datetime.now().strftime("%Y-%m")
    else:
        return competencia


@task
def generate_report(output_directory: str, competencia: str):
    """
    Generate a report
    """

    data_inicio, data_fim = get_month_range(competencia)

    log("Extracting data from BigQuery")

    dataset_com_movimentacao = dados_com_movimentacao(
        data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d")
    )

    log("Movement records extracted")

    dataset_sem_movimentacao = dados_sem_movimentacao(
        data_inicio.strftime("%Y-%m-%d"), data_fim.strftime("%Y-%m-%d")
    )

    log("Medicines without movement records extracted")

    livros = livros_para_gerar()

    log("Reports to be generated extracted")

    template = "pipelines/reports/farmacia_digital/livro_controlados/livro_controlados/template/template.docx"

    log("Generating reports")

    gerar_relatorios(
        df_relacao=livros,
        df_com_mov=dataset_com_movimentacao,
        df_sem_mov=dataset_sem_movimentacao,
        base_directory=output_directory,
        competencia_inicio=data_inicio.strftime("%d.%m.%y"),
        competencia_fim=data_fim.strftime("%d.%m.%y"),
        competencia=competencia,
        template=template,
    )


@task
def upload_report_to_gdrive(folder_path: str, folder_id: str):
    """
    Upload a report to Google Drive
    """

    log("Uploading reports to Google Drive")

    upload_folder_to_gdrive(folder_path, folder_id)
