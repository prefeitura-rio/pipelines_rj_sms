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
        "AGENDAMENTOS",
        "ALERGIAS",
        "ARBOVIROSES",
        "ATENDIMENTOS",
        "CONDICOES",
        "CURATIVOS",
        "ENCAMINHAMENTOS",
        "EQUIPES",
        "HANSENIASE",
        "HIV",
        "IDOSO",
        "INDICADORES",
        "PACIENTES",
        "PLANO",
        "PRENATAL",
        "PRESCRICOES",
        "PROCEDIMENTOS_CLINICOS",
        "PROFISSIONAIS",
        "SAUDE_BUCAL",
        "SAUDECRIANCA",
        "SAUDEMULHER",
        "SIFILIS",
        "SOLICITACAO_EXAMES",
        "TABAGISMO",
        "TESTESRAPIDOS",
        "TUBERCULOSE",
        "UNIDADE",
        "VACINAS",
    ]
