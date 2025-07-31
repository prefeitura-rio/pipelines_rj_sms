# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for the Medilab Api pipeline
"""

from enum import Enum


class medilab_api_constants(Enum):
    """
    Constant values for the medilab flows
    """

    INFISICAL_PATH = "/medlab"
    DATASET_ID = "brutos_medilab"
    GCS_BUCKET_NAME = "medlab_laudos"
