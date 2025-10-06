# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for SISREG procedimentos padronização.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the procedimentos_sisreg flow
    """

    INFISICAL_PATH = "/"
    INFISICAL_API_KEY = "GEMINI_API_KEY"
    GEMINI_MODEL = "gemini-2.5-pro"
    DATASET_ID = "karen__intermediario_sisreg" 
    TABLE_ID = "procedimentos"

    QUERY = """
        SELECT 
        SAFE_CAST(id_procedimento_sisreg AS STRING) AS id_procedimento_sisreg,
        SAFE_CAST(procedimento     AS STRING) AS procedimento,
        SAFE_CAST(procedimento_grupo      AS STRING) AS procedimento_grupo
        FROM `rj-sms-dev.karen__intermediario_sisreg.procedimentos`
        WHERE procedimento_padronizado IS NULL
        AND id_procedimento_sisreg IS NOT NULL
    """