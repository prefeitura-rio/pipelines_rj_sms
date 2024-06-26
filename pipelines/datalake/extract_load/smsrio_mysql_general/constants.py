# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for SMSRio.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/smsrio"
    INFISICAL_DB_URL = "DB_URL"
    DATASET_ID = "brutos_plataforma_smsrio"
    TABLE_ID = {
        "estoque": "estoque_posicao_almoxarifado_aps_dengue",
        "itens_estoque": "materiais_almoxarifado_dengue",
        "contatos_equipe": "equipe_contatos"
    }
