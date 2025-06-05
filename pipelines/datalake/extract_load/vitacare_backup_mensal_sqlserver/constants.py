# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for the Vitacare Historic SQL Server dump pipeline
"""

from enum import Enum


class vitacare_constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/prontuario-vitacare-database"
    INFISICAL_PORT = "DATABASE_PORT"
    INFISICAL_HOST = "DATABASE_HOST"
    INFISICAL_USERNAME = "DATABASE_USER"
    INFISICAL_PASSWORD = "DATABASE_PASSWORD"
    DATASET_ID = "brutos_prontuario_vitacare_historico"
    DB_SCHEMA = "dbo"
    BQ_PARTITION_COLUMN = "extracted_at"

    TABLES_TO_EXTRACT = [
        "ALERGIAS",
        "ATENDIMENTOS",
        "CONDICOES",
        "ENCAMINHAMENTOS",
        "EQUIPES",
        "HIV",
        "INDICADORES",
        "PACIENTES",
        "PRENATAL",
        "PRESCRICOES",
        "PROCEDIMENTOS_CLINICOS",
        "PROFISSIONAIS",
        "SAUDECRIANCA",
        "SAUDEMULHER",
        "SIFILIS",
        "SOLICITACAO_EXAMES",
        "TESTESRAPIDOS",
        "TUBERCULOSE",
        "UNIDADE",
        "VACINAS",
    ]
