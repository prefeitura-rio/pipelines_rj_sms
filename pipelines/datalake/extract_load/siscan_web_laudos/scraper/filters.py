# -*- coding: utf-8 -*-
"""Aplicação de filtros (exame + município + datas) na tela de pesquisa."""

from __future__ import annotations

from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By

from .config import LOGGER
from .driver import (
    clicar_com_retry,
    esperar_overlay_sumir,
    esperar_visivel,
    safe_click,
    wait_until,
)
from .locators import (
    BOTAO_PESQUISAR,
    CAMPO_DATA_FIM,
    CAMPO_DATA_INICIO,
    LUPA_LAUDO,
    MENU_EXAME,
    MENU_GERENCIAR_LAUDO,
    OPCAO_FILTRO_DATA,
    OPCAO_MUNICIPIO,

    OPCAO_EXAME_CITO_COLO,
    OPCAO_EXAME_CITO_MAMA,
    OPCAO_EXAME_HISTO_COLO,
    OPCAO_EXAME_HISTO_MAMA,
    OPCAO_EXAME_MAMO,
    OPCAO_EXAME_MONITORAMENTO_EXTERNO,
)

import time 

def goto_laudo_page(driver: Firefox) -> None:
    """Abre a tela *Gerenciar Laudo* a partir do menu principal."""

    LOGGER.info("Navegando até a tela Gerenciar Laudo…")
    time.sleep(3)
    wait_until(driver, lambda d: d.find_elements(*MENU_EXAME))
    safe_click(driver, MENU_EXAME)

    wait_until(driver, lambda d: d.find_elements(*MENU_GERENCIAR_LAUDO))
    safe_click(driver, MENU_GERENCIAR_LAUDO)

    # Confirma que o radio de filtro por data está presente
    esperar_visivel(driver, OPCAO_FILTRO_DATA)


def _definir_data_js(
    driver: Firefox,
    locator: tuple[str, str],
    valor: str,
) -> None:
    """Insere data em campo *readonly* via JavaScript de forma resiliente."""

    # 1) garante visibilidade do campo
    esperar_visivel(driver, locator)
    campo = driver.find_element(*locator)

    # 2) injeta valor e dispara evento `change`
    driver.execute_script("arguments[0].removeAttribute('readonly')", campo)
    driver.execute_script("arguments[0].value = arguments[1];", campo, valor.replace("/", ""))
    driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", campo)

    # 3) aguarda overlay AJAX desaparecer para evitar ElementClickInterceptedException
    esperar_overlay_sumir(driver, 30)

    # 4) tenta clique normal; se falhar, clique via JS
    if not clicar_com_retry(driver, locator, tentativas=2, scroll=False, timeout=5):
        driver.execute_script("arguments[0].click();", campo)

    # 5) dispara `blur` para validar o campo e confirmar valor
    driver.execute_script("arguments[0].dispatchEvent(new Event('blur'));", campo)

    # 6) certifica-se de que o valor foi realmente aplicado
    wait_until(
        driver,
        lambda d: d.find_element(*locator).get_attribute("value").strip() != "",
    )


def set_filters(driver: Firefox, opcao_exame: str, data_inicio: str, data_fim: str) -> None:
    """Seleciona Mamografia, município e intervalo de datas desejado."""
    LOGGER.info("Aplicando filtros: %s - %s.", data_inicio, data_fim)

    match opcao_exame:
        case "cito_colo":
            safe_click(driver, OPCAO_EXAME_CITO_COLO)
        case "histo_colo":
            safe_click(driver, OPCAO_EXAME_HISTO_COLO)
        case "cito_mama":
            safe_click(driver, OPCAO_EXAME_CITO_MAMA)
        case "histo_mama":
            safe_click(driver, OPCAO_EXAME_HISTO_MAMA)
        case "mamografia":
            safe_click(driver, OPCAO_EXAME_MAMO)
        case "monitoramento_externo":
            safe_click(driver, OPCAO_EXAME_MONITORAMENTO_EXTERNO)

    safe_click(driver, OPCAO_MUNICIPIO)
    safe_click(driver, OPCAO_FILTRO_DATA)

    _definir_data_js(driver, CAMPO_DATA_INICIO, data_inicio)
    _definir_data_js(driver, CAMPO_DATA_FIM, data_fim)

    safe_click(driver, BOTAO_PESQUISAR)

    wait_until(
        driver,
        lambda d: d.find_elements(*LUPA_LAUDO) or d.find_elements(By.CSS_SELECTOR, "table"),
    )
