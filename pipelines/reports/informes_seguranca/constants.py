# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from enum import Enum


class informes_seguranca_constants(Enum):

    EMAIL_PATH = "/datarelay"
    EMAIL_URL = "API_URL"
    EMAIL_TOKEN = "API_TOKEN"
    EMAIL_ENDPOINT = "/data/mailman"
    LOGO_DIT_HORIZONTAL_COLORIDO = "https://storage.googleapis.com/sms_dit_arquivos_publicos/img/dit-horizontal-colorido--300px.png"
    LOGO_SMS_HORIZONTAL_COLORIDO = "https://storage.googleapis.com/sms_dit_arquivos_publicos/img/sms-horizontal-gradiente-sem-sus--150px.png"

    DATASET = "informes_seguranca"
    TABLE = "episodios"

    CID_groups = [
        ["W20", "W49", "Exposição a forças mecânicas inanimadas"],
        ["W50", "W64", "Exposição a forças mecânicas animadas"],
        ["X00", "X09", "Exposição à fumaça, ao fogo e às chamas"],
        ["X40", "X49", "Intoxicação acidental por e exposição a substâncias nocivas"],
        ["X50", "X57", "Excesso de esforços, viagens e privações"],
        ["X58", "X59", "Exposição acidental a outros fatores e aos não especificados"],
        ["X60", "X84", "Lesões autoprovocadas intencionalmente"],
        ["X85", "Y09", "Agressões"],
        ["Y10", "Y34", "Eventos cuja intenção é indeterminada"],
        ["Y35", "Y36", "Intervenções legais e operações de guerra"],
        ["Y40", "Y84", "Complicações de assistência médica e cirúrgica"],
        ["Y85", "Y89", "Sequelas de causas externas de morbidade e de mortalidade"],
    ]
