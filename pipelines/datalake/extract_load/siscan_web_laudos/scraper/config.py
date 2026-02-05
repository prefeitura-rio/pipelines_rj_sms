# -*- coding: utf-8 -*-
"""Constantes centrais de configuração do scraper SIS-CAN (PT-BR)."""

from __future__ import annotations

import logging
from pathlib import Path

from selenium.webdriver.common.by import By

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

# Configurações gerais                                                        #
URL_BASE: str = "https://siscan.saude.gov.br/"

TEMPO_ESPERA_PADRAO: int = 90
TENTATIVAS_LOGIN: int = 2
DIRETORIO_DOWNLOADS: Path = Path.cwd() / "downloads"
HEADLESS_PADRAO: bool = False

ALERTA_MODAL_INESPERADO: tuple[By, str] = (By.CSS_SELECTOR, "div.ui-dialog")
