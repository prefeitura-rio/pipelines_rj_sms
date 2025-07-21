# -*- coding: utf-8 -*-
# pipelines/utils/db.py
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key


def get_mysql_engine(infisical_path: str, secret_name: str, environment: str):
    """
    Recupera URI do Infisical e inicializa engine SQLAlchemy.
    """
    try:
        uri = get_secret_key(
            secret_path=infisical_path, secret_name=secret_name, environment=environment
        )
        log(f"[{environment.upper()}] MySQL URI obtida via secret '{secret_name}'", level="info")
        return create_engine(uri)
    except SQLAlchemyError as e:
        log(
            f"[{environment.upper()}] Erro ao criar engine MySQL: {e}",
            level="error",
            force_discord_forwarding=True,
        )
        raise
