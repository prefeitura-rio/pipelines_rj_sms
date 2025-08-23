# -*- coding: utf-8 -*-
# pylint: disable=C0103

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitacare flows
    """

    LOGIN_URL = "https://inlabs.in.gov.br/logar.php"
    DOWNLOAD_BASE_URL = "https://inlabs.in.gov.br/index.php?p="
    OUTPUT_DIR = './output'
    DOWNLOAD_DIR = './download'
    EMAIL = 'herian.cavalcante@dados.rio'
    PASSWORD = '28102810'