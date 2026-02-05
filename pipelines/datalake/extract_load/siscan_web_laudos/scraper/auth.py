# -*- coding: utf-8 -*-
"""Rotina de autenticação no SIS-CAN (com *retries* e pop-up handling)."""

from __future__ import annotations

from selenium.common.exceptions import TimeoutException, WebDriverException

from . import ScraperError
from .config import LOGGER, TENTATIVAS_LOGIN, URL_BASE
from .driver import safe_click, safe_get, wait_clickable
from .locators import BOTAO_ENTRAR, CAMPO_EMAIL, CAMPO_SENHA

def login(email: str, senha: str, driver):  # type: ignore[arg-type]
    """Efetua login no sistema; tenta novamente em caso de lentidão."""
    LOGGER.info("Iniciando login…")
    tentativa = 0
    while tentativa <= TENTATIVAS_LOGIN:
        tentativa += 1
        try:
            LOGGER.debug(f"Tentativa {tentativa} de {TENTATIVAS_LOGIN + 1}")
            safe_get(driver, URL_BASE)
            LOGGER.debug("URL base acessada com sucesso")
            
            campo_email = wait_clickable(driver, CAMPO_EMAIL)
            LOGGER.debug("Campo de email localizado")
            campo_email.clear()
            campo_email.send_keys(email)
            LOGGER.debug("Email inserido")

            driver.find_element(*CAMPO_SENHA).send_keys(senha)
            LOGGER.debug("Senha inserida")
            
            safe_click(driver, BOTAO_ENTRAR)
            LOGGER.debug("Botão de entrada clicado")
            
            LOGGER.info("Login concluído com sucesso.")
            return
        except (TimeoutException, WebDriverException) as exc:
            LOGGER.warning("Falha no login (%s) – tentativa %d de %d.", exc, tentativa, TENTATIVAS_LOGIN + 1)
    
    LOGGER.error("Falha ao autenticar após %d tentativas.", TENTATIVAS_LOGIN + 1)
    raise ScraperError("Não foi possível autenticar após múltiplas tentativas.")
