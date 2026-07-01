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

EXECUCAO EM PARALELO COM O LEGADO (temporario, ate o C38 desligar os schedules
antigos): este fluxo escreve num dataset SEPARADO (brutos_sisreg_web), mas usa
as MESMAS contas SISREG dos flows legados (SISREG permite 1 sessao por conta).
Para nao competir, este fluxo roda num bloco de TARDE, bem depois dos horarios
do legado (todos de madrugada):
  - legado afastamentos (conta regulador): 00:01
  - legado solicitacoes (conta padrao):    02:00 (+ 20:00 mensal)
  - legado escala       (conta padrao):    05:50
Ressalva honesta: afastamentos (novo) pode levar ~12h; nao ha 24h no dia para
duas execucoes de afastamentos na mesma conta, entao sobreposicoes pontuais com
o legado ainda podem ocorrer. Uma colisao apenas FALHA aquele run (logout ->
ErroBloqueio -> sem escrita); o legado segue como fonte autoritativa e o run
novo tenta de novo no dia seguinte. Sem risco de dados. Resolve de vez no C38.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

_FUSO_SP = pytz.timezone("America/Sao_Paulo")

# ---------------------------------------------------------------------------
# Hora de inicio base (UTC-3) para cada perfil - bloco de TARDE (ver docstring:
# evita os horarios de madrugada do legado enquanto rodam em paralelo).
# ---------------------------------------------------------------------------

# Perfil padrao (conta DATALAKE.ADM): inicia 10:00. Legado padrao roda de
# madrugada (02:00 solicitacoes, 05:50 escala) e ja terminou pela manha.
_INICIO_PADRAO = datetime(2025, 1, 1, 10, 0, tzinfo=_FUSO_SP)

# Perfil regulador (conta CGCR.AMBULATORIOREG): inicia 12:00. O legado
# afastamentos comeca 00:01; ao meio-dia a execucao da madrugada geralmente
# ja terminou. Comecar a tarde tambem faz o run novo (~12h) terminar antes
# do proximo legado das 00:01.
_INICIO_REGULADOR = datetime(2025, 1, 1, 12, 0, tzinfo=_FUSO_SP)

# Intervalo de escalonamento entre conjuntos do MESMO perfil (em minutos).
# 210 min (3.5h) > runtime de solicitacoes (~3.2h) para serializar os conjuntos
# padrao sem sobreposicao. afastamentos (~12h) nao cabe antes de fila_vagas por
# nenhum stagger razoavel; a eventual sobreposicao intra-regulador e tolerada
# (fail-skip, sem risco de dados) ate ajustarmos a cadencia/C38.
_INTERVALO_STAGGER_MIN = 210

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
