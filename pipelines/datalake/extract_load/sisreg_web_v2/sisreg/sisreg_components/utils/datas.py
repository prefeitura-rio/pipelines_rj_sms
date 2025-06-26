# -*- coding: utf-8 -*-
import pandas as pd


def gerar_intervalo_datas(data_inicial: str, data_final: str) -> list[str]:
    """
    Gera um intervalo de datas entre a data inicial e a data final.
    """
    return [data.strftime("%d/%m/%Y") for data in pd.date_range(start=data_inicial, end=data_final)]
