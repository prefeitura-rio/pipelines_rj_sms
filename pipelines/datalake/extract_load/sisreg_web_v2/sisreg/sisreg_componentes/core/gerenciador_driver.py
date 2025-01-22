# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

from prefeitura_rio.pipelines_utils.logging import log

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service


class GerenciadorDriver:
    """
    Classe responsável por configurar e criar a instância do WebDriver do Firefox.
    """

    def __init__(self, caminho_download: str, modo_headless: bool, tempo_carregamento: int = 60):
        """
        Inicializa as configurações para o driver do Firefox.

        Args:
            caminho_download (str): Caminho do diretório para salvar arquivos baixados.
            modo_headless (bool): Se True, executa em modo headless (sem interface).
            tempo_carregamento (int): Tempo máximo em segundos para carregamento de página.
        """
        self.caminho_download = caminho_download
        self.modo_headless = modo_headless
        self.tempo_carregamento = tempo_carregamento
        self.browser = None

    def iniciar_driver(self) -> webdriver.Firefox:
        """
        Inicia o driver com as opções configuradas.

        Returns:
            webdriver.Firefox: Instância do WebDriver pronta para uso.
        """
        opcoes = self._configurar_opcoes()
        servico = Service()
        self.browser = webdriver.Firefox(service=servico, options=opcoes)
        self.browser.set_page_load_timeout(self.tempo_carregamento)
        return self.browser

    def _configurar_opcoes(self) -> FirefoxOptions:
        """
        Configura opções para Firefox, incluindo diretório de download e modo headless.

        Returns:
            FirefoxOptions: Objeto de opções para o navegador Firefox.
        """
        opcoes = FirefoxOptions()

        if self.modo_headless:
            opcoes.add_argument("--headless")

        # Preferências de download
        opcoes.set_preference("browser.download.folderList", 2)
        opcoes.set_preference("browser.download.dir", self.caminho_download)
        opcoes.set_preference("browser.download.useDownloadDir", True)
        opcoes.set_preference("browser.download.manager.showWhenStarting", False)
        opcoes.set_preference("browser.download.manager.showAlertOnComplete", False)
        opcoes.set_preference("browser.download.panel.shown", False)
        opcoes.set_preference("browser.download.manager.scanWhenDone", False)

        # MIME types para não perguntar durante download
        csv_mime_types = (
            "text/csv,"
            "application/csv,"
            "text/plain,"
            "application/vnd.ms-excel,"
            "application/octet-stream"
        )
        opcoes.set_preference("browser.helperApps.neverAsk.saveToDisk", csv_mime_types)

        return opcoes

    def encerrar_driver(self) -> None:
        """
        Encerra o WebDriver, se estiver iniciado.
        """
        if self.browser:
            try:
                self.browser.quit()
            except Exception as e:
                log(f"Falha ao encerrar o navegador: {e}")