# -*- coding: utf-8 -*-
"""
Constants
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump prontuario flows
    """

    DOWNLOAD_DIR = "./temp"
    UNCOMPRESS_FILES_DIR = "./data"
    UPLOAD_PATH = "./upload"
    DICTIONARY_ENCODING = "ISO-8859-1"
    SELECTED_FILES_TO_UNPACK = (
        "cen02._S", "cen02._Sd",
        "cen54._S", "cen54._Sd",
        "cen15._S", "cen15._Sd",
        "alta_clinica._S", "alta_clinica._Sd",
        "triagem._S", "triagem._Sd",
        "intb6._S", "intb6._Sd", 
        "intb0._S", "intb0._Sd",
        "cen70._S", "cen70._Sd",
        "cen62._S", "cen62._Sd",
        "lab63._S", "lab63._Sd",
        "hospub.sql", "prescricao_medica3.sql"
    )
    SELECTED_OPENBASE_TABLES = (
        "cen02._S",
        "cen54._S",
        "cen15._S",
        "alta_clinica._S",
        "triagem._S",
        "intb6._S",
        "intb0._S",
        "cen70._S",
        "cen62._S",
        "lab63._S",
    )
    SELECTED_POSTGRES_TABLES = (
        "hp_rege_evolucao",
        "hp_rege_ralta",
        "hp_rege_receituario",
        "hp_descricao_cirurgia",
        "hp_rege_emerg",
        "hp_prontuario_be",
    )
