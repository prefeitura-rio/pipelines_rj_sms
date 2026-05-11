# -*- coding: utf-8 -*-
"""
Utilities for ArcGIS FeatureServer extraction.
"""
import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


RJ_TZ = ZoneInfo("America/Sao_Paulo")


def stable_json_hash(value: Any) -> str:
    """
    Generate a deterministic SHA-256 hash for a JSON-serializable object.

    The object is serialized with sorted keys and compact separators to ensure
    that equivalent JSON structures always produce the same hash, regardless of
    dictionary key order.

    Args:
        value: Object to serialize and hash.

    Returns:
        SHA-256 hexadecimal hash.
    """
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_feature_hash(properties: Dict[str, Any], geometry: Optional[Dict[str, Any]]) -> str:
    """
    Generate a stable hash for a single ArcGIS feature.

    The hash considers both feature attributes and geometry. It is used to detect
    changes in individual records between ArcGIS extractions.

    Args:
        properties: Feature attributes returned by the ArcGIS endpoint.
        geometry: Feature geometry returned by the ArcGIS endpoint.

    Returns:
        Stable hash representing the feature content.
    """
    return stable_json_hash(
        {
            "properties": properties or {},
            "geometry": geometry or {},
        }
    )


def build_layer_hash(feature_hashes: List[str]) -> str:
    """
    Generate a stable hash for the complete ArcGIS layer extraction.

    The feature hashes are sorted before hashing so the layer signature does not
    depend on the order returned by the ArcGIS API.

    Args:
        feature_hashes: List of hashes generated for each feature.

    Returns:
        Stable hash representing the complete layer content.
    """
    return stable_json_hash({"feature_hashes": sorted(feature_hashes)})


def build_version_id(endpoint_url: str, layer_id: int, layer_hash: str) -> str:
    """
    Build a deterministic version identifier for a loaded ArcGIS layer.

    The version id is derived from the endpoint, layer id and layer hash. This
    allows the same layer content to receive the same identifier across repeated
    executions.

    Args:
        endpoint_url: ArcGIS query endpoint used for extraction.
        layer_id: ArcGIS layer id obtained from metadata.
        layer_hash: Hash representing the complete layer content.

    Returns:
        Short deterministic version id.
    """
    return stable_json_hash(
        {
            "endpoint_url": endpoint_url.rstrip("/"),
            "layer_id": layer_id,
            "layer_hash": layer_hash,
        }
    )[:32]


def now_rj_iso() -> str:
    """
    Return the current datetime in America/Sao_Paulo timezone.

    Returns:
        ISO-formatted datetime string.
    """
    return datetime.now(RJ_TZ).isoformat()


def get_feature_objectid(feature: Dict[str, Any], object_id_field: Optional[str] = None) -> Any:
    """
    Extract the object id from an ArcGIS feature.

    The function first checks the GeoJSON feature id, then the metadata-defined
    object id field, and finally common ArcGIS object id field names.

    Args:
        feature: Feature returned by the ArcGIS endpoint.
        object_id_field: Object id field name informed by layer metadata.

    Returns:
        Feature object id when available; otherwise None.
    """
    properties = feature.get("properties") or {}

    if feature.get("id") is not None:
        return feature.get("id")

    if object_id_field and properties.get(object_id_field) is not None:
        return properties.get(object_id_field)

    for candidate in ("OBJECTID", "OBJECTID_1", "OBJECTID_12", "objectid"):
        if properties.get(candidate) is not None:
            return properties.get(candidate)

    return None


def normalize_feature_row(
    feature: Dict[str, Any],
    endpoint_url: str,
    layer_id: int,
    layer_name: Optional[str],
    layer_hash: str,
    versao_id: str,
    data_extracao: str,
    object_id_field: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Normalize an ArcGIS feature into the raw historical table structure.

    The function extracts known fields used by the Onde Ser Atendido layer,
    preserves the complete feature payload for traceability and stores the
    GeoJSON geometry as text for later conversion in dbt.

    Args:
        feature: Feature returned by the ArcGIS endpoint.
        endpoint_url: ArcGIS query endpoint used for extraction.
        layer_id: ArcGIS layer id obtained from metadata.
        layer_name: ArcGIS layer name obtained from metadata.
        layer_hash: Hash representing the complete layer content.
        versao_id: Deterministic id of the loaded version.
        data_extracao: Extraction datetime.
        object_id_field: Object id field name informed by layer metadata.

    Returns:
        Dictionary representing one row in the historical raw table.
    """
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry")

    feature_hash = build_feature_hash(properties=properties, geometry=geometry)

    return {
        "versao_id": versao_id,
        "data_versao": data_extracao[:10],
        "data_extracao": data_extracao,
        "endpoint_url": endpoint_url.rstrip("/"),
        "layer_id": layer_id,
        "layer_name": layer_name,
        "objectid": get_feature_objectid(feature, object_id_field),
        "global_id": properties.get("GlobalID"),
        "cap": properties.get("CAP"),
        "ap": properties.get("AP"),
        "cnes": properties.get("CNES"),
        "cod_equipe": properties.get("COD_Equipe"),
        "cod_ine": properties.get("COD_INE"),
        "cod_area": properties.get("COD_AREA"),
        "nome_area": properties.get("NOME_AREA"),
        "nome_fantasia": properties.get("NOME_FANTASIA"),
        "tipo_unidade_aps": properties.get("TIPO_UNIDADE_APS"),
        "bairro": properties.get("BAIRRO"),
        "logradouro": properties.get("LOGRADOURO"),
        "numero": properties.get("NUMERO"),
        "complemento": properties.get("COMPLEMENTO"),
        "cod_cep": properties.get("COD_CEP"),
        "telefone": properties.get("TELEFONE"),
        "email": properties.get("E_MAIL"),
        "dt_ativa": properties.get("DT_ATIVA"),
        "tipo_eqp": properties.get("TIPO_EQP"),
        "geometry_geojson": json.dumps(geometry, ensure_ascii=False) if geometry else None,
        "feature_json": json.dumps(feature, ensure_ascii=False),
        "feature_hash": feature_hash,
        "layer_hash": layer_hash,
        "datalake_loaded_at": data_extracao,
    }


def build_metadata_url(endpoint_url: str) -> str:
    """
    Build the ArcGIS layer metadata URL from a query endpoint.

    Args:
        endpoint_url: ArcGIS layer URL or query endpoint URL.

    Returns:
        URL used to request layer metadata.
    """
    endpoint_url = endpoint_url.rstrip("/")
    if endpoint_url.endswith("/query"):
        return endpoint_url[: -len("/query")]
    return endpoint_url


def build_query_url(endpoint_url: str) -> str:
    """
    Build the ArcGIS query URL from a layer or query endpoint URL.

    Args:
        endpoint_url: ArcGIS layer URL or query endpoint URL.

    Returns:
        URL used to request layer features.
    """
    endpoint_url = endpoint_url.rstrip("/")
    if endpoint_url.endswith("/query"):
        return endpoint_url
    return f"{endpoint_url}/query"