# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

from prefeitura_rio.pipelines_utils.logging import log
import os
import time

from selenium.common.exceptions import WebDriverException, TimeoutException

from sisreg_componentes.pages.base_page import BasePage

class PaginaOfertaProgramada(BasePage):
    """
    Página responsável por baixar dados de Oferta Programada pelos profissionais no SISREG.
    """

    def baixar_oferta_programada(self, caminho_download: str) -> None:
        """
        Realiza o download da oferta programada e renomeia o arquivo baixado para 'oferta_programada.csv'.

        Args:
            caminho_download (str): Caminho onde o arquivo CSV será baixado.
        """
        if not os.path.exists(caminho_download):
            os.makedirs(caminho_download, exist_ok=True)

        try:
            # Inicia o download da oferta
            self._iniciar_download()

            # Aguarda até que o download seja concluído
            arquivo_baixado = self._aguardar_download_terminar(caminho_download)
            if not arquivo_baixado:
                log("Nenhum arquivo CSV encontrado após o download.")
                return

            # Renomeia o arquivo baixado
            caminho_renomeado = self._renomear_arquivo(arquivo_baixado, caminho_download)
            log(f"Oferta programada baixada e renomeada para: {caminho_renomeado}")

        except TimeoutException:
            # Se ainda assim ocorrer o TimeoutException fora do try interno,
            # não vamos abortar o processo — o polling de download continua.
            log("TimeoutException ignorada (download direto pode ter sido iniciado).")

        except WebDriverException as e:
            log(f"Erro ao acessar a página de oferta programada: {e}")
            raise

        finally:
            self.voltar_contexto_padrao()

    def _iniciar_download(self) -> None:
        """
        Acessa a URL específica que inicia o download do arquivo CSV.
        Ignora TimeoutException para permitir que o download continue em segundo plano.
        """
        # A URL já dispara o download do arquivo CSV
        url_oferta = (
            "cgi-bin/cons_escalas"
            "?radioFiltro=cpf&status=&dataInicial=&dataFinal="
            "&qtd_itens_pag=50&pagina=&ibge=330455&ordenacao=&clas_lista=ASC"
            "&etapa=EXPORTAR_ESCALAS&coluna="
        )
        try:
            # Passa (None, None) como seletor de espera porque não há elemento para aguardar.
            self.abrir_pagina(url_complemento=url_oferta, seletor_espera=(None, None), tempo_espera=5)
        except TimeoutException:
            # Aqui está a “ignorância” explícita da exceção
            log("TimeoutException ignorada (download direto foi iniciado).")

    def _aguardar_download_terminar(self, caminho_download: str, intervalo_checagem: int = 5) -> str:
        """
        Aguarda até que não haja mais arquivos .part ou .crdownload no diretório.
        Retorna o nome do primeiro .csv encontrado.

        Args:
            caminho_download (str): Diretório onde estão sendo salvos os arquivos.
            intervalo_checagem (int): Intervalo em segundos entre cada checagem.

        Returns:
            str: Caminho completo do primeiro CSV encontrado ou None caso não encontre.
        """
        while True:
            time.sleep(intervalo_checagem)
            arquivos = os.listdir(caminho_download)
            # Verifica se há algum arquivo ainda em download
            em_andamento = any(
                arq.endswith(".part") or arq.endswith(".crdownload") for arq in arquivos
            )
            if not em_andamento:
                # Se não há downloads em andamento, pega o nome do arquivo .csv baixado
                filename = [arq for arq in arquivos if arq.lower().startswith("sisreg_escalas_amb")]
                if filename:
                    return os.path.join(caminho_download, filename[0]) # Espera-se que tenha apenas um arquivo SISREG_ESCALAS_AMB*
                else:
                    return None

    def _renomear_arquivo(self, caminho_arquivo: str, caminho_download: str) -> str:
        """
        Renomeia o arquivo baixado para 'oferta_programada.csv'.

        Args:
            caminho_arquivo (str): Caminho completo do arquivo baixado.
            caminho_download (str): Diretório de download.

        Returns:
            str: Caminho completo do arquivo renomeado.
        """
        novo_caminho = os.path.join(caminho_download, "oferta_programada.csv")

        try:
            os.rename(caminho_arquivo, novo_caminho)
        except OSError as e:
            log(f"Erro ao renomear arquivo {caminho_arquivo} -> {novo_caminho}: {str(e)}")
            return caminho_arquivo

        return novo_caminho