# -*- coding: utf-8 -*-
"""Pacote de raspagem SIS-CAN – versão robusta em PT-BR."""

from __future__ import annotations

from typing import Any, Dict, List

from .config import LOGGER


class ScraperError(RuntimeError):
    """Erro de alto nível do scraper."""


from .auth import login  # noqa: E402

# Importações tardias evitam circularidade
from .driver import init_firefox  # noqa: E402
from .filters import goto_laudo_page, set_filters  # noqa: E402
from .patients import iterate_patients  # noqa: E402

__all__ = ["ScraperError", "run_scraper"]


def run_scraper(
    email: str,
    password: str,
    opcao_exame: str,
    start_date: str,
    end_date: str,
    *,
    headless: bool | None = None,
) -> List[Dict[str, Any]]:
    """Fluxo de ponta a ponta que devolve lista de laudos em dicionários."""
    LOGGER.info(f"Iniciando scraper com opcao_exame={opcao_exame}, período: {start_date} a {end_date}")
    driver = init_firefox(headless=headless)
    try:
        LOGGER.info("Realizando login...")
        login(email, password, driver)
        LOGGER.info("Login realizado com sucesso.")
        
        LOGGER.info("Navegando para página de laudos...")
        goto_laudo_page(driver)
        LOGGER.info("Página de laudos carregada.")
        
        LOGGER.info("Aplicando filtros...")
        set_filters(driver, opcao_exame, start_date, end_date)
        LOGGER.info("Filtros aplicados.")
        
        LOGGER.info("Iterando por pacientes...")
        laudos = iterate_patients(driver, opcao_exame)
        LOGGER.info(f"Scraper finalizado. Total de laudos: {len(laudos)}")
        return laudos
    except Exception as e:
        LOGGER.error(f"Erro durante execução do scraper: {e}", exc_info=True)
        raise
    finally:
        driver.quit()
        LOGGER.info("Driver encerrado.")
