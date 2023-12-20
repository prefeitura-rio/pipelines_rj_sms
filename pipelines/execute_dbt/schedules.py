"""
Schedules for the dbt pipeline
"""
from datetime import timedelta
import pendulum
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

# Define os parâmetros
params_every_day_at_six_am = {
    "command": "run",
    "target": "prod",
    "model": "",
}

every_day_at_six_am = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=pendulum.datetime(2023, 12, 15, 7, 0, 0, tz="America/Sao_Paulo"),
            labels=[
                constants.RJ_SMS_AGENT_LABEL.value,
                params_every_day_at_six_am
            ],
        )
    ]
)