# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

from prefeitura_rio.pipelines_utils.logging import log
from selenium.common.exceptions import WebDriverException

from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_components.utils.navegacao import (
    abrir_pagina,
)


class BasePage:
    """
    Classe base para todas as páginas.
    Assume que `self.browser` e `self.url_base` foram definidos.
    Fornece operações comuns, como:
    - Abrir URL e aguardar elemento
    - Lidar com frames
    - Tratamento de exceções
    """

    def abrir_pagina(
        self, url_complemento: str, seletor_espera: tuple, tempo_espera: int = 30
    ) -> None:
        """
        Abre página usando a função utilitária e aguarda o seletor_espera ficar disponível.

        Args:
            url_complemento (str): Parte final da URL para ser concatenada com self.url_base.
            seletor_espera (tuple): Ex.: (By.ID, "usuario"). Elemento esperado para confirmar carregamento.
            tempo_espera (int): Tempo limite para aguardar o elemento.
        """
        url_completa = f"{self.url_base}{url_complemento}"
        abrir_pagina(self.browser, url_completa, seletor_espera, tempo_espera)

    def voltar_contexto_padrao(self) -> None:
        """
        Tenta voltar para o contexto padrão (fora de frames).
        """
        try:
            self.browser.switch_to.default_content()
        except WebDriverException as e:
            log(f"Falha ao voltar para o contexto padrão do navegador: {str(e)}")

    def mudar_para_frame(self, seletor_frame: tuple, tempo_espera: int = 30) -> None:
        """
        Aguarda até o frame estar disponível e muda para ele.
        """
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        try:
            WebDriverWait(self.browser, tempo_espera).until(
                EC.frame_to_be_available_and_switch_to_it(seletor_frame)
            )
        except TimeoutException as e:
            msg = f"Timeout ao esperar pelo frame {seletor_frame}. Verifique se o site mudou."
            log(msg)
            raise TimeoutException(msg) from e
