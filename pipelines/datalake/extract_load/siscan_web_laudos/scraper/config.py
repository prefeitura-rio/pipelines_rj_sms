"""Constantes centrais de configuração do scraper SIS-CAN (PT-BR)."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from selenium.webdriver.common.by import By

# --------------------------------------------------------------------------- #
# Carrega .env                                                                #
# --------------------------------------------------------------------------- #
load_dotenv()

# --------------------------------------------------------------------------- #
# Logging                                                                     #
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
LOGGER = logging.getLogger("scraper")

# --------------------------------------------------------------------------- #
# Configurações gerais                                                        #
# --------------------------------------------------------------------------- #
URL_BASE: str = "https://siscan.saude.gov.br/"

TEMPO_ESPERA_PADRAO: int = 90  # segundos para WebDriverWait
TENTATIVAS_LOGIN: int = 2  # tentativas *extras* além da primeira

DIRETORIO_DOWNLOADS: Path = Path.cwd() / "downloads"
HEADLESS_PADRAO: bool = False  # mude para True em CI

ALERTA_MODAL_INESPERADO: tuple[By, str] = (By.CSS_SELECTOR, "div.ui-dialog")
