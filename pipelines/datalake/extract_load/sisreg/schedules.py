# -*- coding: utf-8 -*-
"""
Agendamentos do fluxo unificado SISREG.

Um clock por conjunto de dados. Conjuntos do mesmo perfil de credencial
sao escalonados com runs_interval_minutes para nunca disparar ao mesmo
tempo (single-flight R1: apenas um conjunto por conta por vez).

Cadencias:
  - escalas, afastamentos, solicitacoes, fila_vagas: diaria
  - preparos: semanal

Perfis de credencial e seus conjuntos:
  - "padrao"    (/sisreg)        : escalas, preparos, solicitacoes
  - "regulador" (/sisreg_regulacao): afastamentos, fila_vagas

Conjuntos de perfis distintos podem rodar em paralelo (contas independentes).
Conjuntos do mesmo perfil sao serializados pelos starts escalonados.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

_FUSO_SP = pytz.timezone("America/Sao_Paulo")

# ---------------------------------------------------------------------------
# Hora de inicio base (UTC-3) para cada perfil
# ---------------------------------------------------------------------------

# Perfil padrao: inicia 00:01. Conjuntos escalonados de 180 em 180 min.
_INICIO_PADRAO = datetime(2025, 1, 1, 0, 1, tzinfo=_FUSO_SP)

# Perfil regulador: inicia 00:31 para nao colidir com o padrao no mesmo IP.
_INICIO_REGULADOR = datetime(2025, 1, 1, 0, 31, tzinfo=_FUSO_SP)

# Intervalo de escalonamento entre conjuntos do MESMO perfil (em minutos).
# 180 min = 3 h de folga entre inicios para evitar sobreposicao de execucoes
# longas (afastamentos pode levar ~10-15h, por isso usa conta dedicada).
_INTERVALO_STAGGER_MIN = 30

# ---------------------------------------------------------------------------
# Parametros padrao por conjunto
# ---------------------------------------------------------------------------

_PARAMS_BASE = {
    "environment": "prod",
    "dataset_id": C.DATASET_ID_PADRAO,
    "janela_dias": C.JANELA_DIAS,
}


def _params(conjunto: str) -> dict:
    """Monta o dict de parametros para um clock de agendamento."""
    return {**_PARAMS_BASE, "conjunto": conjunto}


# ---------------------------------------------------------------------------
# Clocks - perfil padrao (escalas, preparos, solicitacoes)
# ---------------------------------------------------------------------------

_CLOCKS_PADRAO = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=_INICIO_PADRAO,
    labels=[pipeline_constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=[
        _params("escalas"),
        _params("solicitacoes"),
        # preparos: intervalo semanal, sobrescreve o diario padrao via "interval"
        {**_params("preparos"), "interval": timedelta(days=7)},
    ],
    runs_interval_minutes=_INTERVALO_STAGGER_MIN,
)

# ---------------------------------------------------------------------------
# Clocks - perfil regulador (afastamentos, fila_vagas)
# ---------------------------------------------------------------------------

_CLOCKS_REGULADOR = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=_INICIO_REGULADOR,
    labels=[pipeline_constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=[
        _params("afastamentos"),
        _params("fila_vagas"),
    ],
    runs_interval_minutes=_INTERVALO_STAGGER_MIN,
)

# ---------------------------------------------------------------------------
# Schedule final
# ---------------------------------------------------------------------------

schedule = Schedule(clocks=untuple_clocks(_CLOCKS_PADRAO + _CLOCKS_REGULADOR))
