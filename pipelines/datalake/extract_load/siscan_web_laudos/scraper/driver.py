# -*- coding: utf-8 -*-
# flake8: noqa: E731

"""Inicialização do Firefox + utilitários de espera/clique robustos."""

from __future__ import annotations

from contextlib import suppress
from typing import Any, Callable, TypeVar

from selenium.common.exceptions import (
    ElementClickInterceptedException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver import Firefox, FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.firefox import GeckoDriverManager

from prefeitura_rio.pipelines_utils.logging import log

from .config import DIRETORIO_DOWNLOADS, HEADLESS_PADRAO, TEMPO_ESPERA_PADRAO

import time

_T = TypeVar("_T")


# --------------------------------------------------------------------------- #
# Driver                                                                      #
# --------------------------------------------------------------------------- #
def init_firefox(*, headless: bool | None = None) -> Firefox:
    """Cria e devolve instância configurada do GeckoDriver."""
    if headless is None:
        headless = HEADLESS_PADRAO

    DIRETORIO_DOWNLOADS.mkdir(exist_ok=True)

    opcoes = FirefoxOptions()
    if headless:
        opcoes.add_argument("-headless")

    opcoes.set_preference("browser.download.folderList", 2)
    opcoes.set_preference("browser.download.dir", str(DIRETORIO_DOWNLOADS))
    opcoes.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/json")

    servico = FirefoxService()
    driver = Firefox(service=servico, options=opcoes)
    driver.set_page_load_timeout(TEMPO_ESPERA_PADRAO)
    log(f"Driver Firefox inicializado (headless={headless}).")
    return driver


# --------------------------------------------------------------------------- #
# Esperas genéricas                                                           #
# --------------------------------------------------------------------------- #
def esperar_ate(driver: Firefox, cond: Callable[[Any], _T], timeout: int | None = None) -> _T:
    """Wrapper para ``WebDriverWait`` com timeout padrão."""
    return WebDriverWait(driver, timeout or TEMPO_ESPERA_PADRAO).until(cond)


def esperar_visivel(driver: Firefox, locator: tuple[str, str], timeout: int | None = None):
    """Espera elemento **visível** na página."""
    return esperar_ate(driver, EC.visibility_of_element_located(locator), timeout)


def esperar_clicavel(driver: Firefox, locator: tuple[str, str], timeout: int | None = None):
    """Espera elemento presente **e** clicável."""
    return esperar_ate(driver, EC.element_to_be_clickable(locator), timeout)


def esperar_carregamento(driver: Firefox, timeout: int = 30) -> None:
    """Aguarda ``document.readyState === 'complete'``."""
    esperar_ate(
        driver,
        lambda d: d.execute_script("return document.readyState") == "complete",
        timeout,
    )


# --------------------------------------------------------------------------- #
# Pop-ups                                                                     #
# --------------------------------------------------------------------------- #
def _fechar_janelas_extras(driver: Firefox) -> None:
    """Fecha pop-ups de navegador e mantém foco na janela principal."""
    principal = driver.current_window_handle
    extras = [h for h in driver.window_handles if h != principal]
    for h in extras:
        driver.switch_to.window(h)
        driver.close()
    driver.switch_to.window(principal)


def _fechar_modal_overlay(driver: Firefox) -> None:
    """Fecha modais PrimeFaces/JSF internos da página."""
    seletores = [
        "a.ui-dialog-titlebar-close",
        "button[aria-label='Close']",
        "//button[normalize-space()='Fechar']",
    ]
    for sel in seletores:
        with suppress(NoSuchElementException):
            if sel.startswith("//"):
                driver.find_element(By.XPATH, sel).click()
            else:
                driver.find_element(By.CSS_SELECTOR, sel).click()
            break


# --------------------------------------------------------------------------- #
# Overlay PrimeFaces (máscara cinza)                                         #
# --------------------------------------------------------------------------- #
def esperar_overlay_sumir(driver: Firefox, timeout: int = 30) -> None:
    """
    Aguarda desaparecer a div de máscara ``rich-mpnl-mask-div`` que
    bloqueia cliques enquanto há requisições AJAX.
    """
    cond = lambda d: not any(
        e.is_displayed() for e in d.find_elements(By.CSS_SELECTOR, "div.rich-mpnl-mask-div")
    )
    esperar_ate(driver, cond, timeout)


# --------------------------------------------------------------------------- #
# Ações resilientes                                                           #
# --------------------------------------------------------------------------- #
def clicar_com_retry(
    driver: Firefox,
    locator: tuple[str, str],
    *,
    tentativas: int = 3,
    scroll: bool = True,
    timeout: int | None = None,
) -> bool:
    """Clica no elemento com tentativas extras em caso de *stale element*."""
    for _ in range(tentativas):
        try:
            elem = esperar_clicavel(driver, locator, timeout)
            if scroll:
                driver.execute_script("arguments[0].scrollIntoView(true);", elem)
            elem.click()
            return True
        except ElementClickInterceptedException:  # ➋ trata bloqueio
            log(f"Elemento {locator} bloqueado por overlay - aguardando…")
            try:
                esperar_overlay_sumir(driver, 300)
            except TimeoutException:
                pass
        except (StaleElementReferenceException, TimeoutException):
            log(f"Tentativa extra de clique em {locator}.")
    return False


def clique_seguro(driver: Firefox, locator: tuple[str, str], timeout: int | None = None):
    """Clique tolerante a alertas e *stale element* (mantida para compatibilidade)."""
    clicar_com_retry(driver, locator, tentativas=3, timeout=timeout)


def navegar_seguro(driver: Firefox, url: str):
    """Abre *url* lidando com timeouts, pop-ups e overlays."""
    try:
        driver.get(url)
    except TimeoutException:
        driver.refresh()

    # Aguarda & fecha janelas extras
    try:
        WebDriverWait(driver, 30).until(lambda d: len(d.window_handles) > 1)
    except TimeoutException:
        pass

    time.sleep(4)
    _fechar_janelas_extras(driver)

    # Trata página de mensagens informativas
    if "popupMensagensInformativas" in driver.current_url:
        _fechar_modal_overlay(driver)
        if "popupMensagensInformativas" in driver.current_url:
            driver.back()

    esperar_carregamento(driver)


# --------------------------------------------------------------------------- #
# Aliases p/ compatibilidade                                                  #
# --------------------------------------------------------------------------- #
wait_until = esperar_ate
wait_visible = esperar_visivel
wait_clickable = esperar_clicavel
safe_click = clique_seguro
safe_get = navegar_seguro
