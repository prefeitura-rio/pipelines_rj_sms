# -*- coding: utf-8 -*-
# pylint: disable=C0103

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitacare flows
    """

    LOGIN_URL = "https://inlabs.in.gov.br/logar.php"
    DOWNLOAD_BASE_URL = "https://inlabs.in.gov.br/index.php?p="
    OUTPUT_DIR = "./output"
    DOWNLOAD_DIR = "./download"
    INFISICAL_PASSWORD = "PASSWORD"
    INFISICAL_USERNAME = "USERNAME"
    INFISICAL_PATH = "/inlabs"
    HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
