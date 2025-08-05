# -*- coding: utf-8 -*-
# flake8: noqa: F405, F403

"""Iteração sobre pacientes e extração de laudos detalhados (v2.0)."""

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Set

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .config import LOGGER
from .driver import (
    clicar_com_retry,
    esperar_carregamento,
    esperar_overlay_sumir,
    esperar_visivel,
    safe_click,
)
from .locators import *

_FORMATO_DATA = "%d/%m/%Y"


# --------------------------------------------------------------------------- #
# Helpers                                                                    #
# --------------------------------------------------------------------------- #
def _get_elem(driver: Firefox, locator: tuple[str, str]):
    """
    Retorna o primeiro elemento que corresponde ao locator ou ``None``.

    Args:
        driver: Instância do Firefox WebDriver.
        locator: Tupla ``(By, value)`` do elemento desejado.

    Returns:
        O primeiro ``WebElement`` encontrado ou ``None`` caso inexistente.
    """
    elems: List[WebElement] = driver.find_elements(*locator)
    return elems[0] if elems else None


def get_txt(driver: Firefox, locator: tuple[str, str]) -> str:
    """Texto limpo do elemento – string vazia se não existir ou estiver vazio."""
    elem = _get_elem(driver, locator)
    return elem.text.strip() if elem and elem.text else ""


def get_val(driver: Firefox, locator: tuple[str, str]) -> str:
    """Valor de input/textarea ou texto do elemento; vazio se não existir."""
    elem = _get_elem(driver, locator)
    if not elem:
        return ""
    value = (elem.get_attribute("value") or "").strip()
    return value if value else get_txt(driver, locator)


# --------------------------------------------------------------------------- #
# Página de detalhes                                                          #
# --------------------------------------------------------------------------- #
def _esperar_pagina_detalhe(driver: Firefox) -> None:
    """Garante que campo de protocolo esteja visível antes de extrair dados."""
    esperar_visivel(driver, DET_NPROTO)  # noqa: F403


def _extrair_detalhes(driver: Firefox) -> Dict[str, Any]:
    """
    Extrai todos os 52 campos de detalhes do laudo.

    Se o campo não existir ou estiver vazio, retorna string vazia mantendo
    todas as chaves no dicionário.
    """
    detalhes: Dict[str, Any] = {
        # ----------------------------- Cabeçalho --------------------------- #
        "unidade_nome": get_txt(driver, DET_NOME),  # noqa: F403
        "unidade_uf": get_txt(driver, DET_UF),  # noqa: F403
        "n_exame": get_txt(driver, DET_NEXAME),  # noqa: F403
        "data_solicitacao": (
            datetime.strptime(get_txt(driver, DET_DATA), _FORMATO_DATA).date().isoformat()
            if get_txt(driver, DET_DATA)
            else ""
        ),
        "unidade_cnes": get_txt(driver, DET_CNES),  # noqa: F403
        "unidade_municipio": get_txt(driver, DET_MUNICIPIO),  # noqa: F403
        "n_prontuario": get_txt(driver, DET_NPRONT),  # noqa: F403
        "n_protocolo": get_txt(driver, DET_NPROTO),  # noqa: F403
        # ------------------------------- Paciente -------------------------- #
        "paciente_cartao_sus": get_val(driver, DET_CARTAO_SUS),  # noqa: F403
        "paciente_nome": get_val(driver, DET_NOME_PACIENTE),  # noqa: F403
        "paciente_dt_nasc": get_val(driver, DET_DATA_NASCIMENTO),  # noqa: F403
        "paciente_mae": get_val(driver, DET_MAE),  # noqa: F403
        "paciente_uf": get_val(driver, DET_UF_PACIENTE),  # noqa: F403
        "paciente_bairro": get_val(driver, DET_BAIRRO),  # noqa: F403
        "paciente_endereco_numero": get_val(driver, DET_NUMERO),  # noqa: F403
        "paciente_cep": get_val(driver, DET_CEP),  # noqa: F403
        "paciente_sexo": get_val(driver, DET_SEXO),  # noqa: F403
        "paciente_idade": get_val(driver, DET_IDADE),  # noqa: F403
        "paciente_telefone": get_val(driver, DET_TELEFONE),  # noqa: F403
        "paciente_municipio": get_val(driver, DET_MUNICIPIO_PACIENTE),  # noqa: F403
        "paciente_logradouro": get_val(driver, DET_ENDERECO),  # noqa: F403
        "paciente_endereco_complemento": get_val(driver, DET_COMPLEMENTO),  # noqa: F403
        # ------------------------------ Prestador -------------------------- #
        "prestador_nome": get_val(driver, DET_NOME_PRESTADOR),  # noqa: F403
        "prestador_cnpj": get_val(driver, DET_CNPJ),  # noqa: F403
        "prestador_uf": get_val(driver, DET_UF_PRESTADOR),  # noqa: F403
        "prestador_cnes": get_val(driver, DET_CNES_PRESTADOR),  # noqa: F403
        "data_realizacao": get_val(driver, DET_DATA_RECEBIMENTO),  # noqa: F403
        "prestador_municipio": get_val(driver, DET_MUNICIPIO_PRESTADOR),  # noqa: F403
        # ------------------------------ Indicação -------------------------- #
        "mamografia_tipo": get_val(driver, DET_TIPO_MAMOGRAFIA),  # noqa: F403
        "mamografia_rastreamento_tipo": get_val(
            driver, DET_TIPO_MAMOGRAFIA_RASTREAMENTO
        ),  # noqa: F403
        "achado_exame_clinico": get_val(driver, DET_ACHADO_EXAME_CLINICO),  # noqa: F403
        "achado_exame_direita": get_val(driver, DET_ACHADO_EXAME_DIREITA),  # noqa: F403
        "data_ultima_menstruacao": get_val(driver, DET_DATA_ULTIMA_MENSTRUACAO),  # noqa: F403
        # ------------------------------ Mamografia ------------------------- #
        "numero_filmes": get_val(driver, DET_NUMERO_FILMES),  # noqa: F403
        "mama_direita_pele": get_val(driver, DET_MAMA_DIREITA_PELE),  # noqa: F403
        "tipo_mama_direita": get_val(driver, DET_TIPO_MAMA_DIREITA),  # noqa: F403
        "microcalcificacoes": get_val(driver, DET_MICROCALCIFICACOES),  # noqa: F403
        "linfonodos_axiliares_direita": get_val(
            driver, DET_LINFONODOS_AXILIARES_DIREITA
        ),  # noqa: F403
        "achados_benignos_direita": get_val(driver, DET_ACHADOS_BENIGNOS_DIREITA),  # noqa: F403
        "mama_esquerda_pele": get_val(driver, DET_MAMA_ESQUERDA_PELE),  # noqa: F403
        "tipo_mama_esquerda": get_val(driver, DET_TIPO_MAMA_ESQUERDA),  # noqa: F403
        "linfonodos_axiliares_esquerda": get_val(
            driver, DET_LINFONODOS_AXILIARES_ESQUERDA
        ),  # noqa: F403
        "achados_benignos_esquerda": get_val(driver, DET_ACHADOS_BENIGNOS_ESQUERDA),  # noqa: F403
        # --------------------- Classificação Radiológica ------------------- #
        "classif_radiologica_direita": get_val(
            driver, DET_CLASSIF_RADIOLOGICA_DIREITA
        ),  # noqa: F403
        "classif_radiologica_esquerda": get_val(
            driver, DET_CLASSIF_RADIOLOGICA_ESQUERDA
        ),  # noqa: F403
        "texto_mamas_labels": get_txt(driver, DET_MAMAS_LABELS),  # noqa: F403
        # ---------------------------- Recomendações ------------------------ #
        "recomendacoes": get_val(driver, DET_RECOMENDACOES),  # noqa: F403
        # -------------------------- Observações Gerais --------------------- #
        "observacoes_gerais": get_txt(driver, DET_OBSERVACOES_GERAIS),  # noqa: F403
        # ------------------- Responsável pelo Resultado -------------------- #
        "responsavel_resultado": get_val(driver, DET_RESPONSAVEL_RESULTADO),  # noqa: F403
        "cns_resultado": get_val(driver, DET_CNS_RESULTADO),  # noqa: F403
        "conselho": get_val(driver, DET_CONSELHO),  # noqa: F403
        "data_liberacao_resultado": get_val(driver, DET_DATA_LIBERACAO_RESULTADO),  # noqa: F403
    }

    return detalhes


def _proxima_pagina(driver: Firefox) -> bool:
    try:
        botao = driver.find_element(*BOTAO_PROXIMO)
    except NoSuchElementException:
        return False

    classe = botao.get_attribute("class") or ""
    if "rich-datascr-inact" in classe or botao.get_attribute("onclick") in (None, ""):
        return False

    driver.execute_script("arguments[0].scrollIntoView(true);", botao)
    esperar_overlay_sumir(driver, 30)
    try:
        driver.execute_script("arguments[0].click();", botao)
    except Exception as exc:
        LOGGER.debug("Clique JS falhou (%s); tentando fallback padrão.", exc)
        clicar_com_retry(driver, BOTAO_PROXIMO, scroll=False, timeout=10)

    # Aguarda recarregar / desaparecer overlay AJAX antes de prosseguir
    esperar_overlay_sumir(driver, 90)
    esperar_carregamento(driver)
    WebDriverWait(driver, 90).until(EC.presence_of_element_located(LUPA_LAUDO))
    return True


def iterate_patients(driver: Firefox) -> List[Dict[str, Any]]:
    """Percorre todas as páginas e devolve lista de dicionários de laudos."""
    LOGGER.info("Iniciando iteração sobre pacientes…")
    resultados: List[Dict[str, Any]] = []
    protocolos_vistos: Set[str] = set()
    pagina = 1

    while True:
        LOGGER.info("Processando página %d…", pagina)
        # garante que o overlay AJAX sumiu antes de continuar
        esperar_overlay_sumir(driver, 90)
        botoes = driver.find_elements(*LUPA_LAUDO)
        if not botoes:  # sem laudos = fim da coleta
            LOGGER.info("Nenhum laudo encontrado na página %d - encerrando loop.", pagina)
            break

        novos_na_pagina = 0  # conta quantos protocolos **inéditos** surgem nesta página
        for idx, _ in enumerate(botoes):
            if not clicar_com_retry(
                driver,
                (By.XPATH, f"(//a[@title='Detalhar Laudo'])[{idx + 1}]"),
                scroll=True,
                timeout=10,
            ):
                continue

            try:
                _esperar_pagina_detalhe(driver)
            except TimeoutException:
                driver.back()
                esperar_carregamento(driver)
                continue

            detalhes = _extrair_detalhes(driver)
            if detalhes["n_protocolo"] not in protocolos_vistos:  # verifica nº de protocolo
                resultados.append(detalhes)
                protocolos_vistos.add(detalhes["n_protocolo"])
                novos_na_pagina += 1
                LOGGER.debug("Laudo %s coletado.", detalhes["n_protocolo"])

            # Voltar à lista
            try:
                safe_click(driver, BOTAO_VOLTAR)
            except NoSuchElementException:
                driver.back()
            esperar_carregamento(driver)

        # ------------------------------------------------------------------ #
        # Se **nenhum** protocolo inédito foi encontrado nesta página,       #
        # significa que voltamos a carregá-la (fenômeno na última página).   #
        # Encerra o loop para evitar duplicações ou laço infinito.           #
        # ------------------------------------------------------------------ #
        if novos_na_pagina == 0:
            LOGGER.info(
                "Nenhum laudo inédito na página %d - última página alcançada.",
                pagina,
            )
            break

        if not _proxima_pagina(driver):
            break  # Avança de página; se o botão estiver inativo, também encerra

        pagina += 1

    LOGGER.info("Coleta finalizada: %d laudos únicos.", len(resultados))
    return resultados
