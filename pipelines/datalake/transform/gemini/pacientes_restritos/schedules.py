# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitai Allergies Standardization
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

#####################################
# Vitai Allergies Standardization
#####################################

hci_pacientes_restritos_flow_parameters = [
    {
        "environment": "prod",
        "rename_flow": True,
        "query": """
        with pacientes_suspeita as (
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
        ),
        pacientes_rastreados as (
            SELECT distinct
                id_hci,
                cpf,
            FROM `rj-sms.intermediario_historico_clinico.paciente_restrito`
        )
        select *
        from pacientes_suspeita
        where concat(pacientes_suspeita.id_hci,pacientes_suspeita.cpf) not in (
        select concat(id_hci,cpf)
        from pacientes_rastreados
        )
    """,
    }
]

vitai_alergias_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=hci_pacientes_restritos_flow_parameters,
    runs_interval_minutes=0,
)

hci_pacientes_restritos_daily_update_schedule = Schedule(
    clocks=untuple_clocks(vitai_alergias_clocks)
)
