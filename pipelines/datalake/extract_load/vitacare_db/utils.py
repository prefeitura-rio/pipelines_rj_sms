# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Functions for Vitacare db pipeline
"""
try:
    import pyodbc
except ImportError:
    pass
from prefeitura_rio.pipelines_utils.logging import log


def create_db_connection(
    database_host: str,
    database_port: int,
    database_user: str,
    database_password: str,
    database_name: str,
    autocommit: bool = False,
):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={database_host},{database_port};"
        f"DATABASE={database_name};"
        f"UID={database_user};"
        f"PWD={database_password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    redacted = conn_str.replace(database_password, "[REDACTED]")
    log(f"Connection String: {redacted}")

    return pyodbc.connect(conn_str, autocommit=autocommit)
