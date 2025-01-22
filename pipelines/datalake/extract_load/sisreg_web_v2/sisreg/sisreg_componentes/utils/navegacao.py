# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

from prefeitura_rio.pipelines_utils.logging import log

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


def abrir_pagina(browser, url: str, seletor_espera: tuple, tempo_espera: int = 30) -> None:
    """
    Abre a URL especificada e aguarda até que o elemento definido em 'seletor_espera'
    esteja presente na página, se for fornecido.

    Args:
        browser (webdriver): Instância do WebDriver.
        url (str): URL completa a ser carregada.
        seletor_espera (tuple): (By, "localizador") ou (None, None) se não houver elemento a esperar.
        tempo_espera (int): Tempo limite em segundos para aguardar o elemento.
    """
    try:
        log(f"Abrindo página: {url}")
        browser.get(url)

        if seletor_espera[0] is not None and seletor_espera[1] is not None:
            WebDriverWait(browser, tempo_espera).until(
                EC.presence_of_element_located(seletor_espera)
            )
    except TimeoutException as e:
        msg = f"Timeout ao esperar a página '{url}' carregar o elemento {seletor_espera}"
        log(msg)
        raise TimeoutException(msg) from e
