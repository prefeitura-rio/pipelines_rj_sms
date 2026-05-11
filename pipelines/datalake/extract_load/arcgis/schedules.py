# -*- coding: utf-8 -*-
# pylint: disable=C0103, import-error
"""
Schedules for ArcGIS FeatureServer extract/load flows.
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.arcgis.constants import (
    constants as arcgis_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

RJ_TZ = pytz.timezone("America/Sao_Paulo")
LABELS = [constants.RJ_SMS_AGENT_LABEL.value]

TABLES_CONFIG = [
    {
        "project": "ArcGIS - Onde Ser Atendido - Equipes de APS",
        "endpoint_url": (
            "https://services1.arcgis.com/OlP4dGNtIcnD3RYf/arcgis/rest/services/"
            "OSA2/FeatureServer/1/query"
        ),
        "dataset_id": arcgis_constants.DATASET_ID.value,
        "table_id": "osa__equipes_historico",
        "versions_table_id": arcgis_constants.VERSIONS_TABLE_ID.value,
        "frequency": "weekly",
        "where_clause": arcgis_constants.DEFAULT_WHERE.value,
        "out_fields": arcgis_constants.DEFAULT_OUT_FIELDS.value,
        "page_size": arcgis_constants.DEFAULT_PAGE_SIZE.value,
        "output_format": arcgis_constants.DEFAULT_FORMAT.value,
        "environment": "prod",
        "rename_flow": True,
        "if_exists": "append",
        "if_storage_data_exists": "append",
        "dump_mode": "append",
    },
]


def build_param(config: dict) -> dict:
    """
    Build flow parameters from one ArcGIS layer configuration.

    Args:
        config: Layer configuration from TABLES_CONFIG.

    Returns:
        Parameter dictionary used by the Prefect schedule.
    """
    return {
        "endpoint_url": config["endpoint_url"],
        "dataset_id": config["dataset_id"],
        "table_id": config["table_id"],
        "versions_table_id": config["versions_table_id"],
        "where_clause": config.get("where_clause", arcgis_constants.DEFAULT_WHERE.value),
        "out_fields": config.get("out_fields", arcgis_constants.DEFAULT_OUT_FIELDS.value),
        "page_size": config.get("page_size", arcgis_constants.DEFAULT_PAGE_SIZE.value),
        "output_format": config.get("output_format", arcgis_constants.DEFAULT_FORMAT.value),
        "environment": config.get("environment", "prod"),
        "rename_flow": config.get("rename_flow", True),
        "if_exists": config.get("if_exists", "append"),
        "if_storage_data_exists": config.get("if_storage_data_exists", "append"),
        "dump_mode": config.get("dump_mode", "append"),
    }


def unpack_params(frequency: str) -> list:
    """
    Select scheduled layer configurations by frequency.

    Args:
        frequency: Frequency label used in TABLES_CONFIG.

    Returns:
        List of flow parameter dictionaries for the requested frequency.
    """
    return [build_param(config) for config in TABLES_CONFIG if config["frequency"] == frequency]


daily_params = unpack_params("daily")
weekly_params = unpack_params("weekly")
monthly_params = unpack_params("monthly")

BASE_START_DATE = datetime(2026, 5, 11, 6, 0, tzinfo=RJ_TZ)

daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=daily_params,
    runs_interval_minutes=1,
)

weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=weekly_params,
    runs_interval_minutes=1,
)

monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=4),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=monthly_params,
    runs_interval_minutes=1,
)

arcgis_feature_server_daily_schedule = Schedule(clocks=untuple_clocks(daily_clocks))
arcgis_feature_server_weekly_schedule = Schedule(clocks=untuple_clocks(weekly_clocks))
arcgis_feature_server_monthly_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))
arcgis_feature_server_combined_schedule = Schedule(
    clocks=untuple_clocks(daily_clocks + weekly_clocks + monthly_clocks)
)
