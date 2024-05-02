# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants
"""
from enum import Enum


class constants(Enum):
    TARGET_FOLDER_ID = "1ClVX2dbWNxg2yHtXyH9mtEICrUpsy8bN"

    PROJECT_NAME = {
        "prod": "rj-sms",
        "staging": "rj-sms-dev",
        "dev": "rj-sms-dev",
    }
    DATASET_NAME = "gerenciamento__monitoramento_vitacare"
    TABLE_NAME = "result"
