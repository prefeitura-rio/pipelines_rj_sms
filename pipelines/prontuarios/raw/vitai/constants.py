# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Vitai Raw Data Extraction
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/prontuario-vitai"
    INFISICAL_KEY = "TOKEN"
    INFISICAL_API_USERNAME = "API_USERNAME"
    INFISICAL_API_PASSWORD = "API_PASSWORD"

    API_CNES_TO_URL = {
        "5717256": "http://api.token.hmrg.vitai.care/api/v1/",
        "2298120": "https://api.token.hmas.vitai.care/api/v1/",
        "2295407": "https://api.token.hmrf.vitai.care/api/v1/",
        "6716938": "https://api.token.cerbarra.vitai.care/api/v1/",
        "6716849": "https://apitokencerleblon.vitai.care/api/v1/",
        "6512925": "https://api.token.upaalemao.vitai.care/api/v1/",
        "6507409": "https://api.token.uparocinha.vitai.care/api/v1/",
        "6575900": "https://api.token.upacdd.vitai.care/api/v1/",
        "6680704": "https://api.token.upacostabarros.vitai.care/api/v1/",
        "0932280": "https://api.token.upadc.vitai.care/api/v1/",
        "6631169": "https://api.token.upaed.vitai.care/api/v1/",
        "6598544": "https://api.token.upaj23.vitai.care/api/v1/",
        "6661904": "https://api.token.upamadureira.vitai.care/api/v1/",
        "6421482": "https://api.token.upamanguinhos.vitai.care/api/v1/",
        "7101856": "https://api.token.upamb.vitai.care/api/v1/",
        "6938124": "https://api.token.upapaciencia.vitai.care/api/v1/",
        "6742831": "https://api.token.upasc.vitai.care/api/v1/",
        "6926177": "https://api.token.upasepetiba.vitai.care/api/v1/",
        "6487815": "https://api.token.upavk.vitai.care/api/v1/",
        "2270242": "https://api.token.hmbr.vitai.care/api/v1/",
        "2269481": "https://api.token.hmpiedade.vitai.care/api/v1/",
        "2291266": "https://api.token.hmfst.vitai.care/api/v1/",
        "2269341": "https://api.token.hmj.vitai.care/api/v1/",
        "2270269": "https://api.token.hmmc.vitai.care/api/v1/",
        "7110162": "https://api.token.uparm.vitai.care/api/v1/",
    }

    API_CNES_TO_MIN_DATE = {
        "6716938": {"pacientes": "2012-08-24", "diagnostico": "2015-08-03"},
        "6487815": {"pacientes": "2020-01-10", "diagnostico": "2020-01-10"},
        "6926177": {"pacientes": "2020-04-16", "diagnostico": "2020-04-15"},
        "6742831": {"pacientes": "2015-04-01", "diagnostico": "2015-07-30"},
        "6507409": {"pacientes": "2020-04-22", "diagnostico": "2020-04-22"},
        "6938124": {"pacientes": "2020-03-11", "diagnostico": "2020-03-11"},
        "6421482": {"pacientes": "2020-12-08", "diagnostico": "2020-12-07"},
        "7101856": {"pacientes": "2020-10-03", "diagnostico": "2020-10-01"},
        "6661904": {"pacientes": "2020-06-26", "diagnostico": "2020-02-10"},
        "6598544": {"pacientes": "2020-03-03", "diagnostico": "2020-03-02"},
        "6631169": {"pacientes": "2020-12-19", "diagnostico": "2020-12-19"},
        "0932280": {"pacientes": "2022-05-18", "diagnostico": "2022-05-19"},
        "6680704": {"pacientes": "2020-01-29", "diagnostico": "2020-01-29"},
        "6512925": {"pacientes": "2020-05-20", "diagnostico": "2020-05-20"},
        "6575900": {"pacientes": "2015-11-30", "diagnostico": "2015-11-30"},
        "2295407": {"pacientes": "2018-02-12", "diagnostico": "2018-02-12"},
        "5717256": {"pacientes": "2016-08-19", "diagnostico": "2016-08-22"},
        "2298120": {"pacientes": "2016-10-31", "diagnostico": "2016-11-02"},
        "6716849": {"pacientes": "2020-04-01", "diagnostico": "2020-04-01"},
        "2270242": {"pacientes": "2023-03-28", "diagnostico": "2023-02-21"},
        "2269481": {"pacientes": "2022-08-25", "diagnostico": "2022-08-25"},
        "2291266": {"pacientes": "2023-03-26", "diagnostico": "2023-02-26"},
        "2269341": {"pacientes": "2023-01-17", "diagnostico": "2023-01-17"},
        "2270269": {"pacientes": "2023-03-22", "diagnostico": "2023-02-04"},
        "7110162": {"pacientes": "2015-05-01", "diagnostico": "2015-07-22"},
    }
