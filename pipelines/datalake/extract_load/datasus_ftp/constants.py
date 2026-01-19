# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for DataSUS dump.
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the dump cnes flows
    """

    DATASUS_FTP_SERVER = "ftp.datasus.gov.br"
    DATASUS_ENDPOINT = {
        "cnes": {"file_path": "/cnes", "base_file": "BASE_DE_DADOS_CNES"},
        "cbo": {"file_path": "/dissemin/publicos/CNES/200508_/Auxiliar"},
    }
