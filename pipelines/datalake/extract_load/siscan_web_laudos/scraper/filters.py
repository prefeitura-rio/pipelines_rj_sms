# -*- coding: utf-8 -*-
"""Aplicação de filtros (exame + município + datas) na tela de pesquisa."""

from __future__ import annotations

from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from prefeitura_rio.pipelines_utils.logging import log

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
)

import time 

def goto_laudo_page(driver: Firefox) -> None:
    """Abre a tela *Gerenciar Laudo* a partir do menu principal."""

    log("Navegando até a tela Gerenciar Laudo…")
    time.sleep(3)
    
    log("Aguardando menu de exame…")
    wait_until(driver, lambda d: d.find_elements(*MENU_EXAME))
    log("Clicando no menu de exame…")
    safe_click(driver, MENU_EXAME)

    log("Aguardando opção Gerenciar Laudo…")
    wait_until(driver, lambda d: d.find_elements(*MENU_GERENCIAR_LAUDO))
    log("Clicando em Gerenciar Laudo…")
    safe_click(driver, MENU_GERENCIAR_LAUDO)

    # Confirma que o radio de filtro por data está presente
    log("Verificando se opção de filtro por data está visível…")
    esperar_visivel(driver, OPCAO_FILTRO_DATA)
    log("Tela Gerenciar Laudo carregada com sucesso.")


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
    log("Aplicando filtros: %s - %s.", data_inicio, data_fim)

    log("Selecionando tipo de exame: %s", opcao_exame)
    match opcao_exame:
        case "cito_colo":
            log("Clicando em Citologia de Colo")
            safe_click(driver, OPCAO_EXAME_CITO_COLO)
        case "histo_colo":
            log("Clicando em Histologia de Colo")
            safe_click(driver, OPCAO_EXAME_HISTO_COLO)
        case "cito_mama":
            log("Clicando em Citologia de Mama")
            safe_click(driver, OPCAO_EXAME_CITO_MAMA)
        case "histo_mama":
            log("Clicando em Histologia de Mama")
            safe_click(driver, OPCAO_EXAME_HISTO_MAMA)
        case "mamografia":
            log("Clicando em Mamografia")
            safe_click(driver, OPCAO_EXAME_MAMO)

    log("Selecionando município")
    safe_click(driver, OPCAO_MUNICIPIO)
    
    log("Selecionando filtro por data")
    safe_click(driver, OPCAO_FILTRO_DATA)

    log("Definindo data de início: %s", data_inicio)
    _definir_data_js(driver, CAMPO_DATA_INICIO, data_inicio)
    
    log("Definindo data de fim: %s", data_fim)
    _definir_data_js(driver, CAMPO_DATA_FIM, data_fim)

    log("Clicando em Pesquisar")
    safe_click(driver, BOTAO_PESQUISAR)

    log("Aguardando resultados da pesquisa")
    wait_until(
        driver,
        lambda d: d.find_elements(*LUPA_LAUDO) or d.find_elements(By.CSS_SELECTOR, "table"),
    )
    log("Filtros aplicados com sucesso")
