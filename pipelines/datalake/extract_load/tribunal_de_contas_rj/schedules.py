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
    {
        "environment": "prod",
        # FIXME: queremos um schedule? Acho melhor esse flow ser
        #        chamado "manualmente" pelo flow do D.O.
    },
]


# clocks = generate_dump_api_schedules(
#     interval=timedelta(days=1),
#     start_date=datetime(2025, 6, 6, 6, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
#     labels=[
#         constants.RJ_SMS_AGENT_LABEL.value,
#     ],
#     flow_run_parameters=flow_parameters,
#     runs_interval_minutes=3,
# )

# daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
