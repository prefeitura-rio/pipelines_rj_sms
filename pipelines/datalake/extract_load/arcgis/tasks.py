# -*- coding: utf-8 -*-
# pylint: disable=import-error
"""
Tasks for ArcGIS FeatureServer extract/load flows.
"""
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from google.cloud import bigquery

from pipelines.datalake.extract_load.arcgis.utils import (
    ArcGISFeatureContext,
    ArcGISLayerVersionContext,
    build_feature_hash,
    build_layer_hash,
    build_metadata_url,
    build_query_url,
    build_version_id,
    normalize_feature_row,
    now_rj_iso,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def fetch_arcgis_layer_metadata(endpoint_url: str) -> Dict[str, Any]:
    """
    Fetch metadata for an ArcGIS layer.

    The metadata is used to identify the layer name, layer id, geometry type,
    spatial reference and object id field.

    Args:
        endpoint_url: ArcGIS layer URL or query endpoint URL.

    Returns:
        ArcGIS layer metadata payload.
    """
    url = build_metadata_url(endpoint_url)
    response = requests.get(url, params={"f": "json"}, timeout=120)
    response.raise_for_status()

    payload = response.json()
    if "error" in payload:
        raise RuntimeError(payload["error"])

    return payload


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def fetch_arcgis_layer_features(
    endpoint_url: str,
    where_clause: str = "1=1",
    out_fields: str = "*",
    page_size: int = 2000,
    output_format: str = "pgeojson",
) -> List[Dict[str, Any]]:
    """
    Fetch all features from an ArcGIS layer using paginated requests.

    The task keeps requesting pages until the endpoint returns fewer records
    than the configured page size or no records.

    Args:
        endpoint_url: ArcGIS layer URL or query endpoint URL.
        where_clause: ArcGIS SQL-like filter expression.
        out_fields: Comma-separated list of attributes to return.
        page_size: Number of records requested per page.
        output_format: ArcGIS response format.

    Returns:
        List of features returned by the ArcGIS endpoint.
    """
    url = build_query_url(endpoint_url)
    offset = 0
    features: List[Dict[str, Any]] = []

    while True:
        params = {
            "where": where_clause,
            "outFields": out_fields,
            "returnGeometry": "true",
            "f": output_format,
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        response = requests.get(url, params=params, timeout=180)
        response.raise_for_status()

        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"])

        page_features = payload.get("features", [])
        if not page_features:
            break

        features.extend(page_features)
        log(f"ArcGIS: {len(features)} features extraídas")

        if len(page_features) < page_size:
            break

        offset += page_size

    return features


def build_features_dataframe(
    features: List[Dict[str, Any]],
    context: ArcGISFeatureContext,
) -> pd.DataFrame:
    """
    Build the historical feature DataFrame for a loaded ArcGIS layer.
    """
    rows = [
        normalize_feature_row(
            feature=feature,
            context=context,
        )
        for feature in features
    ]

    return pd.DataFrame(rows)


def build_version_dataframe(
    features_df: pd.DataFrame,
    metadata: Dict[str, Any],
    context: ArcGISLayerVersionContext,
) -> pd.DataFrame:
    """
    Build the ArcGIS layer version control DataFrame.

    Args:
        features_df: Historical features DataFrame produced for the layer.
        metadata: ArcGIS layer metadata payload.
        context: Version context used to identify and audit the extraction.

    Returns:
        DataFrame with one row describing the layer extraction version.
    """
    objectids = pd.to_numeric(features_df.get("objectid"), errors="coerce")

    return pd.DataFrame(
        [
            {
                "versao_id": context.versao_id,
                "endpoint_url": context.endpoint_url.rstrip("/"),
                "layer_id": metadata.get("id"),
                "layer_name": metadata.get("name"),
                "layer_hash": context.layer_hash,
                "features_count": len(features_df),
                "min_objectid": objectids.min() if not objectids.empty else None,
                "max_objectid": objectids.max() if not objectids.empty else None,
                "arcgis_geometry_type": metadata.get("geometryType"),
                "arcgis_spatial_reference_wkid": (
                    metadata.get("spatialReference") or {}
                ).get("wkid"),
                "arcgis_last_edit_date": (
                    metadata.get("editingInfo") or {}
                ).get("lastEditDate"),
                "data_extracao": context.data_extracao,
                "status": "pending",
                "observacao": None,
                "datalake_loaded_at": context.data_extracao,
                "dataset_id": context.dataset_id,
                "table_id": context.table_id,
            }
        ]
    )

@task
def build_arcgis_dataframes(
    features: List[Dict[str, Any]],
    metadata: Dict[str, Any],
    endpoint_url: str,
    dataset_id: str,
    table_id: str,
) -> Dict[str, pd.DataFrame]:
    """
    Build feature and version DataFrames from an ArcGIS extraction.

    The feature DataFrame contains one row per feature for the historical raw
    table. The version DataFrame contains one row with extraction metadata and
    the layer hash used for change detection.

    Args:
        features: Features returned by the ArcGIS endpoint.
        metadata: ArcGIS layer metadata payload.
        endpoint_url: ArcGIS query endpoint used for extraction.
        dataset_id: Destination dataset id.
        table_id: Destination historical table id.

    Returns:
        Dictionary with `features` and `version` DataFrames.
    """
    if not features:
        raise ValueError("A camada ArcGIS não retornou features para carga.")

    data_extracao = now_rj_iso()
    layer_id = metadata.get("id") or 0

    feature_hashes = [
        build_feature_hash(
            properties=feature.get("properties") or {},
            geometry=feature.get("geometry"),
        )
        for feature in features
    ]

    layer_hash = build_layer_hash(feature_hashes)
    versao_id = build_version_id(
        endpoint_url=endpoint_url,
        layer_id=layer_id,
        layer_hash=layer_hash,
    )

    context = ArcGISFeatureContext(
        endpoint_url=endpoint_url,
        layer_id=layer_id,
        layer_name=metadata.get("name"),
        layer_hash=layer_hash,
        versao_id=versao_id,
        data_extracao=data_extracao,
        object_id_field=metadata.get("objectIdFieldName"),
    )

    features_df = build_features_dataframe(
        features=features,
        context=context,
    )

    version_context = ArcGISLayerVersionContext(
        endpoint_url=endpoint_url,
        dataset_id=dataset_id,
        table_id=table_id,
        layer_hash=layer_hash,
        versao_id=versao_id,
        data_extracao=data_extracao,
    )

    versions_df = build_version_dataframe(
        features_df=features_df,
        metadata=metadata,
        context=version_context,
    )

    return {
        "features": features_df,
        "version": versions_df,
    }


@task(max_retries=2, retry_delay=timedelta(seconds=30))
def get_last_loaded_layer_hash(
    dataset_id: str,
    versions_table_id: str,
    endpoint_url: str,
    environment: str,
) -> Optional[str]:
    """
    Retrieve the most recent loaded layer hash from the versions table.

    This hash is used to decide whether the current extraction represents a new
    version of the ArcGIS layer or whether no content change occurred.

    Args:
        dataset_id: Base dataset id used by the Data Lake upload task.
        versions_table_id: Table that stores ArcGIS layer version records.
        endpoint_url: ArcGIS query endpoint used for extraction.
        environment: Execution environment.

    Returns:
        Last loaded layer hash when available; otherwise None.
    """
    project_id = "rj-sms-dev" if environment.lower() == "dev" else "rj-sms"
    dataset_ref = f"{dataset_id}_staging"
    full_ref = f"{project_id}.{dataset_ref}.{versions_table_id}"

    query = f"""
        SELECT layer_hash
        FROM `{full_ref}`
        WHERE endpoint_url = @endpoint_url
            AND status = 'loaded'
        ORDER BY data_extracao DESC
        LIMIT 1
    """

    client = bigquery.Client(project=project_id)

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "endpoint_url",
                    "STRING",
                    endpoint_url.rstrip("/"),
                ),
            ]
        )
        rows = list(client.query(query, job_config=job_config).result())
    except Exception as exc:  # pylint: disable=broad-except
        log(f"Tabela de versões ainda indisponível ou sem leitura: {exc}", level="warning")
        return None

    if not rows:
        return None

    return rows[0]["layer_hash"]


@task
def resolve_changed_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    last_loaded_hash: Optional[str],
) -> Dict[str, Any]:
    """
    Decide whether the current ArcGIS extraction should be persisted.

    When the current layer hash differs from the last loaded hash, the feature
    DataFrame is kept and the version status is set to `loaded`. Otherwise, the
    feature DataFrame is emptied and only a version record with
    `skipped_no_change` is persisted.

    Args:
        dataframes: Dictionary with feature and version DataFrames.
        last_loaded_hash: Last loaded hash found in the versions table.

    Returns:
        Dictionary with resolved DataFrames and change flag.
    """
    features_df = dataframes["features"]
    version_df = dataframes["version"].copy()

    current_hash = version_df.iloc[0]["layer_hash"]
    changed = current_hash != last_loaded_hash

    if changed:
        version_df.loc[:, "status"] = "loaded"
        version_df.loc[:, "observacao"] = "Nova versão carregada."
        log("ArcGIS FeatureServer: alteração identificada. Nova versão será carregada.")
    else:
        version_df.loc[:, "status"] = "skipped_no_change"
        version_df.loc[:, "observacao"] = "Sem alteração em relação à última versão carregada."
        features_df = pd.DataFrame()
        log("ArcGIS FeatureServer: nenhuma alteração identificada.")

    return {
        "features": features_df,
        "version": version_df,
        "changed": changed,
    }
