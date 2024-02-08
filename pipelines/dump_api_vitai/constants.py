# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
"""
Constants for Vitai.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/prontuario-vitai"
    INFISICAL_KEY = "TOKEN"
    DATASET_ID = "brutos_prontuario_vitai"
    ENDPOINT = {
        "posicao": "https://apidw.vitai.care/api/dw/v1/produtos/smsrj/saldoAtual",
        "movimento": "https://apidw.vitai.care/api/dw/v1/movimentacaoProduto/smsrj/query/dataMovimentacao",  # noqa: E501
    }
