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
    SELECTED_BASE_FILES = (
        "alta_clinica._S",
        "alta_clinica._Sd",
        "cen02._S",
        "cen02._Sd",
        "cen54._S",
        "cen54._Sd",
        "cen15._S",
        "cen15._Sd",
        "cen62._S",
        "cen62._Sd",
        "cen70._S",
        "cen70._Sd",
        "intb6._S",
        "intb6._Sd",
        "intb0._S",
        "intb0._Sd",
        "intc0._Sd",
        "intc0._S",   
        "lab63._S",
        "lab63._Sd",
        "neo04._S",
        "neo04._Sd",
        "neo21._S",
        "neo21._Sd",
        "neo22._S",
        "neo22._Sd",
        "neo25._S",
        "neo25._Sd",
        "neo26._S",
        "neo26._Sd",
        "neo47._S",
        "neo47._Sd",
        "neo46._S",
        "neo46._Sd",
        "neo55._S",
        "neo55._Sd",
        "neo57._S",
        "neo57._Sd",
        "triagem._S",
        "triagem._Sd",
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
        "intc0._S",
        "neo04._S",
        "neo21._S",
        "neo22._S",
        "neo25._S",
        "neo26._S",
        "neo47._S",
        "neo46._S",
        "neo55._S",
        "neo57._S",
    )
    SELECTED_HOSPUB_TABLES = (
        "hp_rege_evolucao",
        "hp_rege_ralta",
        "hp_rege_receituario",
        "hp_descricao_cirurgia",
        "hp_rege_emerg",
        "hp_prontuario_be",
    )
    SELECTED_PRESCRICAO_TABLES = (
        "atendimento",
        "dieta",
        "fa_just_med",
        "fa_just_medicamento",
        "medicamento",
        "medicamento_ccih",
        "medicamento_sala",
        "medico",
        "paciente",
        "prescricao",
    )
