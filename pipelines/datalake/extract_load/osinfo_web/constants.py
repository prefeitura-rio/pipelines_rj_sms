# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for SIH.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump sih flows
    """

    OSINFO_HEADER = {
        "despesa": [
            "Num_Seq",
            "Nome_Os",
            "Nome_Unidade",
            "Num_Contrato",
            "Ano_Mes_Ref",
            "Tipo_Documento",
            "Cod_Bancario",
            "Cnpj",
            "Razao",
            "Cpf",
            "Nome",
            "Num_Documento",
            "Serie",
            "Descricao",
            "Documeto_Existe",
            "Data_Emissao",
            "Data_Vencimento",
            "Data_Pagamento",
            "Data_Apuracao",
            "Valor_Documento",
            "Valor_Pago",
            "Despesa",
            "Rubrica",
            "Pmt_Mes",
            "Pmt_Total",
            "Flg_Justificativa",
            "Conta_Corrente",
            "Nome_Agencia",
            "Nome_Banco",
            "Validado",
        ]
    }
