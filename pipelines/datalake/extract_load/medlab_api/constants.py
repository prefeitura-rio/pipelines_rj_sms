# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for the Medlab Api pipeline
"""

from enum import Enum


class medlab_api_constants(Enum):
    """
    Constant values for the medlab flows
    """

    INFISICAL_PATH = "/medlab"
    DATASET_ID = "brutos_medlab"
    GCS_BUCKET_NAME = "medlab_laudos"
