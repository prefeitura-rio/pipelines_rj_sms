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
    driver = init_firefox(headless=headless)
    try:
        login(email, password, driver)
        goto_laudo_page(driver)
        set_filters(driver, opcao_exame, start_date, end_date)
        return iterate_patients(driver, opcao_exame)
    finally:
        driver.quit()
        LOGGER.info("Driver encerrado.")
