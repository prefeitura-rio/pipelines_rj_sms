# -*- coding: utf-8 -*-
"""Rotina de autenticação no SIS-CAN (com *retries* e pop-up handling)."""

from __future__ import annotations

from selenium.common.exceptions import TimeoutException, WebDriverException

from prefeitura_rio.pipelines_utils.logging import log

from . import ScraperError
from .config import TENTATIVAS_LOGIN, URL_BASE
from .driver import safe_click, safe_get, wait_clickable
from .locators import BOTAO_ENTRAR, CAMPO_EMAIL, CAMPO_SENHA

def login(email: str, senha: str, driver):  # type: ignore[arg-type]
    """Efetua login no sistema; tenta novamente em caso de lentidão."""
    log("Iniciando login…")
    tentativa = 0
    while tentativa <= TENTATIVAS_LOGIN:
        tentativa += 1
        try:
            log(f"Tentativa {tentativa} de {TENTATIVAS_LOGIN + 1}")
            safe_get(driver, URL_BASE)
            log("URL base acessada com sucesso")
            
            campo_email = wait_clickable(driver, CAMPO_EMAIL)
            log("Campo de email localizado")
            campo_email.clear()
            campo_email.send_keys(email)
            log("Email inserido")

            driver.find_element(*CAMPO_SENHA).send_keys(senha)
            log("Senha inserida")
            
            safe_click(driver, BOTAO_ENTRAR)
            log("Botão de entrada clicado")
            
            log("Login concluído com sucesso.")
            return
        except (TimeoutException, WebDriverException) as exc:
            log("Falha no login (%s) – tentativa %d de %d.", exc, tentativa, TENTATIVAS_LOGIN + 1)
    
    log("Falha ao autenticar após %d tentativas.", TENTATIVAS_LOGIN + 1)
    raise ScraperError("Não foi possível autenticar após múltiplas tentativas.")
