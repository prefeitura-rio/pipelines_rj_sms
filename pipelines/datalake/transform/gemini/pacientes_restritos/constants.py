# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for SMSRio.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/"
    INFISICAL_API_KEY = "GEMINI_API_KEY"
    GEMINI_MODEL = "gemini-2.0-flash"
    DATASET_ID = "intermediario_historico_clinico"
    TABLE_ID = "paciente_restrito"
    QUERY = """
        with pacientes as (
        SELECT distinct
            id_hci,
            cpf,
            clinical_motivation,
        FROM `rj-sms.app_historico_clinico.episodio_assistencial` as ep
        where (
            lower(clinical_motivation) like "%hiv positivo%"
                or lower(clinical_motivation) like "%soropositivo%"
                or lower(clinical_motivation) like "%aids%"
                or lower(clinical_motivation) like "%hiv+%"
                or lower(clinical_motivation) like "%imunodeficiência%humana%"
            )
        and exibicao.paciente_restrito is false
        )
        select * from pacientes
        order by rand()
        limit 10
    """
