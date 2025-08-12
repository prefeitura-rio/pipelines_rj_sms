# -*- coding: utf-8 -*-
"""
Tarefas para o Web Scraping do SISREG.
"""


import os

# bibliotecas padrão
from datetime import datetime, timedelta
from typing import Tuple

# bibliotecas prefect
from prefect.engine.signals import FAIL

# módulos internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg_web_v2.constants import (
    constants as sisreg_constants,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.core.sisreg import (
    Sisreg,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.utils.path_utils import (  # noqa
    definir_caminho_absoluto,
)
from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import add_load_date_column, get_secret_key


# TAREFAS SISREG --------------------------
# tarefa 1: login
@task(max_retries=5, retry_delay=timedelta(minutes=3), nout=2)
def login_sisreg(environment: str, caminho_relativo) -> Tuple[Sisreg, str]:
    """
    Realiza o login no site do SISREG.

    args:
        environment (str): Ambiente de execução.

    returns:
        tuple: Instância do SISREG e caminho do diretório de download.
    """
    username = get_secret_key.run(
        secret_path=sisreg_constants.INFISICAL_PATH.value,
        secret_name=sisreg_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=sisreg_constants.INFISICAL_PATH.value,
        secret_name=sisreg_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    log("Fazendo login no site do SISREG.")

    caminho_download = definir_caminho_absoluto(caminho_relativo)

    sisreg = Sisreg(
        usuario=username,
        senha=password,
        caminho_download=caminho_download,
    )

    sisreg.fazer_login()
    return sisreg, caminho_download


# tarefa 2: obter dados
@task(max_retries=5, retry_delay=timedelta(minutes=3))
def raspar_pagina(sisreg: Sisreg, caminho_download: str, metodo: str) -> str:
    """
    Esta tarefa recebe o nome de um método (ex: 'baixar_oferta_programada') e o executa
    dinamicamente na instância do Sisreg. Em seguida, retorna o caminho do arquivo baixado.

    Exemplo de uso:
        extrair_pagina(sisreg, "/caminho/para/download", "baixar_oferta_programada")

    Args:
        sisreg (Sisreg): Instância do SISREG.
        caminho_download (str): Diretório absoluto onde os dados serão baixados.
        metodo (str): Nome do método na classe Sisreg a ser chamado (ex: 'baixar_oferta_programada')

    Returns:
        str: Caminho do arquivo CSV gerado (ex: oferta_programada.csv).
    """
    # Garante que a classe Sisreg tenha o método solicitado
    if not hasattr(sisreg, metodo):
        raise AttributeError(f"O método '{metodo}' não existe na classe Sisreg.")

    # Chama dinamicamente o método de download (ex: sisreg.baixar_oferta_programada())
    funcao_download = getattr(sisreg, metodo)
    funcao_download()

    # Se o método começar com 'baixar_', extraímos o resto do nome para gerar o CSV
    if metodo.startswith("baixar_"):
        nome_base = metodo.replace("baixar_", "")
    else:
        nome_base = metodo

    # Nomeia o arquivo gerado (algo como: oferta_programada.csv)
    novo_caminho_arquivo = os.path.join(caminho_download, f"{nome_base}.csv")

    return novo_caminho_arquivo


# tarefa 3: encerrar webdriver
@task(max_retries=5, retry_delay=timedelta(minutes=3))
def sisreg_encerrar(sisreg: Sisreg) -> None:
    """
    Encerra a sessão no SISREG.

    args:
        sisreg (SisregApp): Instância do sisreg.
    """
    sisreg.encerrar()


# TAREFAS GESTÃO E TRANSFORMAÇÃO ARQUIVOS --------------------------
# tarefa 1 - renomear arquivo
@task()
def nome_arquivo_adicionar_data(arquivo_original: str, diretorio_destino: str = None) -> str:
    """
    Renomeia o arquivo informado para incluir a data atual no nome.
    A nova nomenclatura segue o padrão: {nome_base}_{data}.{extensao}

    args:
        arquivo_original (str): Caminho completo do arquivo original.
        diretorio_destino (str, opcional): Diretório onde o arquivo renomeado será salvo.

    returns:
        str: Caminho do arquivo renomeado.
    """
    if not os.path.exists(arquivo_original):
        msg = f"Arquivo original não encontrado: {arquivo_original}"
        log(msg, level="error")
        raise FAIL(msg)

    # define o diretório de destino: se informado, usa-o; caso contrário, mantém o diretório original # noqa
    novo_diretorio = diretorio_destino if diretorio_destino else os.path.dirname(arquivo_original)

    data_sufixo = datetime.now().strftime("%Y-%m-%d")
    nome_base, extensao = os.path.splitext(os.path.basename(arquivo_original))
    novo_nome = f"{nome_base}_{data_sufixo}{extensao}"
    novo_caminho = os.path.join(novo_diretorio, novo_nome)

    try:
        os.rename(arquivo_original, novo_caminho)
        log(f"Arquivo renomeado para: {novo_caminho}")

    except Exception as e:
        msg = f"Erro ao renomear o arquivo: {e}"
        log(msg, level="error")
        raise FAIL(msg)

    return novo_caminho


# tarefa 2 - ajustar dados para upload no lake
@task()
def transform_data(file_path: str) -> str:
    """
    Transforma os dados do arquivo informado e converte para parquet.

    args:
        file_path (str): Caminho do arquivo de entrada.

    returns:
        str: Caminho do arquivo convertido para parquet.
    """

    if not os.path.exists(file_path):
        log(f"File not found: {file_path}", level="error")
        raise FAIL(f"File not found: {file_path}")

    # adiciona a coluna de data de carga
    file_path = add_load_date_column.run(input_path=file_path, sep=";")

    # ajusta o cabeçalho para o formato do datalake
    file_path = conform_header_to_datalake(file_path=file_path, file_type="csv", csv_sep=";")

    # converte dados para formato parquet
    parquet_file_path = convert_to_parquet(file_path=file_path, csv_sep=";", encoding="utf-8")

    log(f"Caminho do arquivo parquet: {parquet_file_path}")
    log(f"Tipo caminho do arquivo parquet: {type(parquet_file_path)}")

    return parquet_file_path
