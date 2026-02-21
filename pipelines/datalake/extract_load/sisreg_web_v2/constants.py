# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
"""
Constantes para o SISREG.
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump sisreg flows
    """

    INFISICAL_PATH = "/sisreg"
    INFISICAL_VITACARE_USERNAME = "SISREG_USER"
    INFISICAL_VITACARE_PASSWORD = "SISREG_PASSWORD"
    DATASET_ID = "brutos_sisreg_v2"


METODO_TABELA = {
    "baixar_oferta_programada": "oferta_programada",
    "baixar_afastamentos": "afastamentos",
    "baixar_executados": "executados",
    # adicione novos métodos e os respectivos nomes de suas tabelas aqui
}
