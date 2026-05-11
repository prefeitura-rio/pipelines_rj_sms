# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for ArcGIS FeatureServer extract/load flows.
"""
from enum import Enum


class constants(Enum):
    """
    Default values for ArcGIS FeatureServer flows.
    """

    DATASET_ID = "brutos_arcgis"
    VERSIONS_TABLE_ID = "arcgis_layer_versions"
    DEFAULT_PAGE_SIZE = 2000
    DEFAULT_WHERE = "1=1"
    DEFAULT_OUT_FIELDS = "*"
    DEFAULT_FORMAT = "pgeojson"
