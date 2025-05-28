# -*- coding: utf-8 -*-
"""Constantes centrais de configuração do scraper SIS-CAN (PT-BR)."""

from __future__ import annotations

from pathlib import Path

from selenium.webdriver.common.by import By

# Configurações gerais                                                        #
URL_BASE: str = "https://siscan.saude.gov.br/"

TEMPO_ESPERA_PADRAO: int = 90
TENTATIVAS_LOGIN: int = 2
DIRETORIO_DOWNLOADS: Path = Path.cwd() / "downloads"
HEADLESS_PADRAO: bool = False

ALERTA_MODAL_INESPERADO: tuple[By, str] = (By.CSS_SELECTOR, "div.ui-dialog")
