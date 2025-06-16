# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501


from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.core.gerenciador_driver import (
    GerenciadorDriver,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.pages.base_page import (
    BasePage,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.pages.pagina_afastamentos import (
    PaginaAfastamentos,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.pages.pagina_executados import (
    PaginaExecutados,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.pages.pagina_login import (
    PaginaLogin,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.pages.pagina_oferta_programada import (
    PaginaOfertaProgramada,
)


class Sisreg(BasePage, PaginaLogin, PaginaOfertaProgramada, PaginaAfastamentos, PaginaExecutados):
    """
    Classe principal que orquestra a interação com o SISREG.
    Agrega e gerencia as páginas do sistema.
    """

    URL_BASE = "https://sisregiii.saude.gov.br/"

    def __init__(
        self,
        usuario: str,
        senha: str,
        caminho_download: str,
        modo_headless: bool = False,
        tempo_carregamento: int = 60,
    ):
        """
        Construtor que prepara a aplicação SISREG.

        Args:
            usuario (str): Usuário para login no SISREG.
            senha (str): Senha para login no SISREG.
            caminho_download (str): Caminho onde os arquivos serão baixados.
            modo_headless (bool): Se True, executa o navegador em modo headless.
            tempo_carregamento (int): Tempo máximo de carregamento de páginas (segundos).
        """
        self.usuario = usuario
        self.senha = senha
        self.caminho_download = caminho_download

        # Instancia e inicia o driver
        self._gerenciador = GerenciadorDriver(
            caminho_download=self.caminho_download,
            modo_headless=modo_headless,
            tempo_carregamento=tempo_carregamento,
        )
        self.browser = self._gerenciador.iniciar_driver()

        self.url_base = self.URL_BASE

    def encerrar(self) -> None:
        """
        Encerra o navegador (WebDriver).
        """
        log("Encerrando SISREG...")
        self._gerenciador.encerrar_driver()
