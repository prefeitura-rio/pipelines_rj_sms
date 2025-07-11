# -*- coding: utf-8 -*-
# pylint: disable=C0103,C0301
"""
Schedules for the smsrio dump url
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1EkYfxuN2bWD_q4OhHL8hJvbmQKmQKFrk0KLf6D7nKS4/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Sheet1",
        "table_id": "estabelecimento_auxiliar",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "csv_delimiter": "|",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "CID de Risco Gestacional",
        "rename_flow": True,
        "table_id": "cids_risco_gestacional",
        "url": "https://docs.google.com/spreadsheets/d/1hpTi-pwJlYqOcdor__v4ONg6yH-2H73qClesdTmGyeo/edit",  # noqa: E501
        "url_type": "google_sheet",
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/176u8I3xlAW7mFN3M0QADZ2b6jU0iUAfWs4CvKd5rhJU/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Municipios",
        "table_id": "municipios_rio",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1WdUZ0vB5Sr2wVhA6bKKwOaQiauLB3r4A4iL2O1E2XwU/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Lista",
        "table_id": "procedimentos_ser",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/176u8I3xlAW7mFN3M0QADZ2b6jU0iUAfWs4CvKd5rhJU/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Bairros",
        "table_id": "bairros_rio",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1b4HBj85ZqAXhS36hI0TZG5y4Obsav0utzRkcOiHMBC8/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Classificações CID",
        "table_id": "projeto_c34_cids",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1b4HBj85ZqAXhS36hI0TZG5y4Obsav0utzRkcOiHMBC8/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Classificações Procedimentos",
        "table_id": "projeto_c34_procedimentos",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1Y7ji6HL5a2fJGIX-olRCVdv98oQ5nWHaBYUNAF7jTKg/edit?gid=1600307210",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "relacao_materiais",
        "dataset_id": "brutos_sheets",
        "table_id": "material_mestre",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1Y7ji6HL5a2fJGIX-olRCVdv98oQ5nWHaBYUNAF7jTKg/edit?gid=1600307210",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "UNIDADES POR PROGRAMA",
        "dataset_id": "brutos_sheets",
        "table_id": "material_cobertura_programas",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "csv_delimiter": ";",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "Farmácia Digital",
        "rename_flow": True,
        "table_id": "gerenciamento_acesso_looker_farmacia",
        "url": "https://docs.google.com/spreadsheets/d/1VCtUiRFxMy1KatBfw9chUppPEIPSGDup9wiwfm9-Djo",  # noqa
        "url_type": "google_sheet",
    },
    {
        "csv_delimiter": ";",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "ATAS E PROCESSOS VIGENTES",
        "rename_flow": True,
        "table_id": "compras_atas_processos_vigentes",
        "url": "https://docs.google.com/spreadsheets/d/1fi7MzF0S4OfTym-fjpLR51wIvTLq-WCE706N6eEEWys",  # noqa
        "url_type": "google_sheet",
    },
    {
        "csv_delimiter": ";",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "Farmácias",
        "rename_flow": True,
        "table_id": "aps_farmacias",
        "url": "https://docs.google.com/spreadsheets/d/17b4LRwQ5F5K5jCdeO0_K1NzqoQV9JqSOAuA0HZhG0uI",  # noqa
        "url_type": "google_sheet",
    },
    {
        "csv_delimiter": ";",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "Dado",
        "rename_flow": True,
        "table_id": "sigtap_procedimentos",
        "url": "https://docs.google.com/spreadsheets/d/14kBPPc9VdeMHlNbUVc_C6PLUrwL73yrQfyl14rjrGuA",  # noqa
        "url_type": "google_sheet",
    },
    {
        "csv_delimiter": ";",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "sheet1",
        "rename_flow": True,
        "table_id": "profissionais_cns_cpf_aux",
        "url": "https://docs.google.com/spreadsheets/d/15OhN69JH6GdRK1Ixvr9P-eTSG03lM6qD7hobcT4Ilow",  # noqa
        "url_type": "google_sheet",
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1P4JbgfSpaTyE7Qh3fzSeHIxlxcHclbJWOTFcRE-DwgE/edit",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Procedimentos",
        "table_id": "assistencial_procedimento",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": "|",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "csv_delimiter": "|",
        "dataset_id": "brutos_sheets",
        "environment": "prod",
        "gsheets_sheet_name": "Contas",
        "rename_flow": True,
        "table_id": "usuarios_permitidos_hci",
        "url": "https://docs.google.com/spreadsheets/d/1jwp5rV3Rwr2NGQy60YQgF47PSFuTcVpE8uKhcrODnRs/edit",  # noqa: E501
        "url_type": "google_sheet",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=3,
)

daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
