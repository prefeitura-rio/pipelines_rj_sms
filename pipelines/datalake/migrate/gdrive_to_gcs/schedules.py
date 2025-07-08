# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    # ===============================
    # VITACARE DATABASE BACKUPS
    # ===============================
    {
        "folder_id": "1VUdm8fixnUs_dJrcflsNvzXIGPX6e-2r",
        "bucket_name": "vitacare_backups_gdrive",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    # ===============================
    # VITACARE INFORMES MENSAL
    # ===============================
    {
        "folder_id": "1H_49fLhbT0bWYk8gBKLOdYHgT_xODpg7",
        "bucket_name": "vitacare_informes_mensais_gdrive",
        "environment": "prod",
        "owner_email": "storage02healthbr@gmail.com",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    # # ===============================
    # # SIH
    # # ===============================
    # {
    #     "folder_id": "1cOi0sZgjso-2Oiv93R62T060zd1XzeMu",
    #     "bucket_name": "sih_gdrive",
    #     "environment": "prod",
    #     "last_modified_date": "M-0",
    #     "rename_flow": True,
    # },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 3, 17, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=1,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
