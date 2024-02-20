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
        '5717256' : 'http://api.token.hmrg.vitai.care/api/v1/',
        '2298120' : 'https://api.token.hmas.vitai.care/api/v1/',
        '2295407' : 'https://api.token.hmrf.vitai.care/api/v1/',
        '6716938' : 'https://api.token.cerbarra.vitai.care/api/v1/',
        '6512925' : 'https://api.token.upaalemao.vitai.care/api/v1/',
        '6507409' : 'https://api.token.uparocinha.vitai.care/api/v1/',
        '6575900' : 'https://api.token.upacdd.vitai.care/api/v1/',
        '6680704' : 'https://api.token.upacostabarros.vitai.care/api/v1/',
        '0932280' : 'https://api.token.upadc.vitai.care/api/v1/',
        '6631169' : 'https://api.token.upaed.vitai.care/api/v1/',
        '6598544' : 'https://api.token.upaj23.vitai.care/api/v1/',
        '6661904' : 'https://api.token.upamadureira.vitai.care/api/v1/',
        '6421482' : 'https://api.token.upamanguinhos.vitai.care/api/v1/',
        '7101856' : 'https://api.token.upamb.vitai.care/api/v1/',
        '6938124' : 'https://api.token.upapaciencia.vitai.care/api/v1/',
        '6742831' : 'https://api.token.upasc.vitai.care/api/v1/',
        '6926177' : 'https://api.token.upasepetiba.vitai.care/api/v1/',
        '6487815' : 'https://api.token.upavk.vitai.care/api/v1/',
        '6716849' : 'https://apitokencerleblon.vitai.care/api/v1/',
    }
