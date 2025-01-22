# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

import os
import sys

def definir_caminho_absoluto(caminho_relativo: str) -> str:
    """
    Retorna o caminho absoluto, considerando que o caminho_relativo é
    informado em relação ao local onde o main.py está executando.

    Args:
        caminho_relativo (str): O caminho relativo informado pelo usuário.

    Returns:
        str: O caminho absoluto correspondente ao caminho_relativo.
    """
    
    # localiza o diretório do arquivo main.py em tempo de execução
    diretorio_main = os.path.dirname(os.path.abspath(sys.modules['__main__'].__file__))

    # retorna o caminho absoluto final
    caminho_resolvido = os.path.abspath(
        os.path.join(diretorio_main, caminho_relativo)
    )

    os.makedirs(caminho_resolvido, exist_ok=True)

    return caminho_resolvido