# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Datalake Data Extraction and Upload to HCI
"""
from enum import Enum


class constants(Enum):
    INFISICAL_PATH = "/smsrio"
    INFISICAL_API_USERNAME = "API_USERNAME"
    INFISICAL_API_PASSWORD = "API_PASSWORD"
    ENDPOINT = {
        "profissional_saude": "mrg/professionals",
        "profissional_saude_equipe_desatualizada": "mrg/professionals",
        "equipe_profissional_saude": "mrg/teams",
    }
    ENDPOINT_REQUIRED_FIELDS = {
        "mrg/professionals": ["id_cbo", "id_registro_conselho"],
        "mrg/teams": [
            "id_ine",
            "nome_referencia",
            "id_cnes",
            "id_equipe_tipo",
            "equipe_tipo_descricao",
            "id_area",
            "area_descricao",
            "ultima_atualizacao_profissionais",
            "ultima_atualizacao_infos_equipe",
        ],
    }
