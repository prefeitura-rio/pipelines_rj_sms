# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

from prefeitura_rio.pipelines_utils.logging import log
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_componentes.core.gerenciador_driver import (
    GerenciadorDriver,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_componentes.pages.pagina_login import (
    PaginaLogin,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_componentes.pages.pagina_oferta_programada import (
    PaginaOfertaProgramada,
)


class SisregApp:
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
        modo_headless: bool = True,
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

        # Instancia as páginas, compartilhando o mesmo browser e URL_BASE
        self._pagina_login = PaginaLogin(self.browser, self.URL_BASE)
        self._pagina_oferta = PaginaOfertaProgramada(self.browser, self.URL_BASE)

    def fazer_login(self) -> None:
        """
        Executa a ação de login no SISREG.
        """
        self._pagina_login.fazer_login(self.usuario, self.senha)

    def extrair_executados(
        self, data_inicial: str, data_final: str, nome_arquivo_saida: str
    ) -> None:
        """
        Extrai dados de executados no SISREG para o intervalo de datas especificado.
        """
        self._pagina_executados.extrair_executados(
            data_inicial=data_inicial, data_final=data_final, nome_arquivo_saida=nome_arquivo_saida
        )

    def extrair_oferta_programada(self, caminho_download: str) -> None:
        """
        Realiza o download da oferta programada.
        """
        self._pagina_oferta.baixar_oferta_programada(caminho_download)

    def extrair_afastamentos(self, caminho_entrada: str, nome_arquivo_saida: str) -> None:
        """
        Extrai dados de afastamentos para CPFs listados em um arquivo de entrada.
        """
        self._pagina_afastamentos.extrair_afastamentos(caminho_entrada, nome_arquivo_saida)

    def encerrar(self) -> None:
        """
        Encerra o navegador (WebDriver).
        """
        log("Encerrando SISREG...")
        self._gerenciador.encerrar_driver()
