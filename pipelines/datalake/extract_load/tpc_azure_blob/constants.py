# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
# flake8: noqa: E501
"""
Constants for TPC.
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump tpc flow
    """

    INFISICAL_PATH = "/tpc"
    INFISICAL_TPC_TOKEN = "TOKEN"

    CONTAINER_NAME = "datalaketpc"
    BLOB_PATH = {
        "posicao": "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/estoque_local/estoque_local.csv",
        "pedidos": "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/pedidos_depositante/pedidos_depositante.csv",
        "recebimento": "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/recebimento_documental/recebimento_documental.csv",
    }

    DATASET_ID = "brutos_estoque_central_tpc"
