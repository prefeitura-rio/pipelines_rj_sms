# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the subpav dump pipeline de extração e carga de dados do SUBPAV.

Este módulo organiza as tabelas por frequência de execução (mensal, semanal e por hora),
gera os parâmetros padrão para cada tabela e define os clocks usados pelos agendamentos do Prefect.
Não recomendado uso por hora sem aprovação.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.subpav_mysql.constants import (
    constants as subpav_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

RJ_TZ = pytz.timezone("America/Sao_Paulo")
LABELS = [constants.RJ_SMS_AGENT_LABEL.value]

# -----------
# Parâmetros
# -----------
# Args: (table_id, schema, frequencia,extra):
# - frequencia: "daily", "monthly", "weekly" ou "per_hour".
# - extra: "dump_mode","if_exists","if_storage_data_exists","relative_date_filter"
# -infisical_path: "/plataforma-subpav" (novo bd subpav)

TABELAS_CONFIG = [
    # Principal
    (
        "bairros",
        "subpav_principal",
        "monthly",
        {
            "dump_mode": "overwrite",
            "if_exists": "replace",
            "if_storage_data_exists": "replace",
        },
    ),
    # Indicadores
    ("indicadores", "subpav_indicadores", "monthly"),
    # CNES APS
    ("cbos", "subpav_cnes", "monthly"),
    ("categorias_profissionais", "subpav_cnes", "monthly"),
    ("cbos_categorias_profissionais", "subpav_cnes", "monthly"),
    (
        "competencias",
        "subpav_cnes",
        "weekly",
        {
            "dump_mode": "overwrite",
            "if_exists": "replace",
            "if_storage_data_exists": "replace",
        },
    ),
    ("equipes", "subpav_cnes", "monthly"),
    (
        "equipes_profissionais",
        "subpav_cnes",
        "monthly",
        {
            "relative_date_filter": "D-60",
        },
    ),
    ("equipes_tipos", "subpav_cnes", "monthly"),
    ("horarios_atendimentos", "subpav_cnes", "monthly"),
    ("profissionais", "subpav_cnes", "monthly"),
    (
        "profissionais_unidades",
        "subpav_cnes",
        "monthly",
        {
            "relative_date_filter": "D-60",
        },
    ),
    ("unidades", "subpav_cnes", "monthly"),
    ("unidades_auxiliares", "subpav_cnes", "monthly"),
    ("unidades_estruturas_fisicas", "subpav_cnes", "monthly"),
    # Acesso Mais Seguro AMS
    # Prefixo notificacoes_ são as transacionais
    ("consequencias", "subpav_acesso_mais_seguro", "monthly"),
    ("eventos", "subpav_acesso_mais_seguro", "monthly"),
    ("locais", "subpav_acesso_mais_seguro", "monthly"),
    ("notificacoes", "subpav_acesso_mais_seguro", "monthly"),
    ("notificacoes_consequencias", "subpav_acesso_mais_seguro", "monthly"),
    ("notificacoes_equipes", "subpav_acesso_mais_seguro", "monthly"),
    ("notificacoes_eventos", "subpav_acesso_mais_seguro", "monthly"),
    ("notificacoes_protagonistas", "subpav_acesso_mais_seguro", "monthly"),
    ("protagonistas", "subpav_acesso_mais_seguro", "monthly"),
    ("riscos", "subpav_acesso_mais_seguro", "monthly"),
    ("status", "subpav_acesso_mais_seguro", "monthly"),
    # SISARE
    # Mensais auxiliares e diárias transacionais
    ("altas", "subpav_altas_referenciadas", "daily"),
    ("altas_arquivos", "subpav_altas_referenciadas", "daily"),
    ("altas_medicamentos", "subpav_altas_referenciadas", "daily"),
    ("altas_pendentes", "subpav_altas_referenciadas", "daily"),
    ("altas_pendentes_referenciadas", "subpav_altas_referenciadas", "daily"),
    ("apgars", "subpav_altas_referenciadas", "monthly"),
    ("comorbidades", "subpav_altas_referenciadas", "monthly"),
    ("dashboard", "subpav_altas_referenciadas", "monthly"),
    ("dashboard_feedback", "subpav_altas_referenciadas", "monthly"),
    ("desfechos_gestacao", "subpav_altas_referenciadas", "monthly"),
    ("desfechos_internacao", "subpav_altas_referenciadas", "monthly"),
    ("exames", "subpav_altas_referenciadas", "monthly"),
    ("feedbacks", "subpav_altas_referenciadas", "daily"),
    ("feridas_cirurgicas", "subpav_altas_referenciadas", "monthly"),
    ("formas_entrada", "subpav_altas_referenciadas", "monthly"),
    ("formularios", "subpav_altas_referenciadas", "monthly"),
    ("formularios_exames", "subpav_altas_referenciadas", "monthly"),
    ("formularios_motivos_internacao", "subpav_altas_referenciadas", "monthly"),
    ("gestantes", "subpav_altas_referenciadas", "daily"),
    ("gestantes_patologias", "subpav_altas_referenciadas", "daily"),
    ("gestantes_rn", "subpav_altas_referenciadas", "daily"),
    ("gestantes_uti", "subpav_altas_referenciadas", "daily"),
    ("gestantes_uti_hemoderivados", "subpav_altas_referenciadas", "daily"),
    ("gestantes_uti_hemorragias", "subpav_altas_referenciadas", "daily"),
    ("gestantes_uti_ventilatorios", "subpav_altas_referenciadas", "daily"),
    ("hemoderivados", "subpav_altas_referenciadas", "monthly"),
    ("histerectomia_causas", "subpav_altas_referenciadas", "monthly"),
    ("histerectomia_tipos", "subpav_altas_referenciadas", "monthly"),
    ("hemoderivados", "subpav_altas_referenciadas", "monthly"),
    ("historico_edicao", "subpav_altas_referenciadas", "daily"),
    ("intercorrencias", "subpav_altas_referenciadas", "monthly"),
    ("internacoes", "subpav_altas_referenciadas", "daily"),
    ("internacoes_comorbidades", "subpav_altas_referenciadas", "daily"),
    ("internacoes_diagnosticos", "subpav_altas_referenciadas", "daily"),
    ("internacoes_exames", "subpav_altas_referenciadas", "daily"),
    ("internacoes_intercorrencias", "subpav_altas_referenciadas", "daily"),
    ("log", "subpav_altas_referenciadas", "daily"),
    ("log_dashboard", "subpav_altas_referenciadas", "daily"),
    ("log_tabelas", "subpav_altas_referenciadas", "monthly"),
    ("motivos_alta", "subpav_altas_referenciadas", "monthly"),
    ("motivos_clinicos", "subpav_altas_referenciadas", "monthly"),
    ("motivos_internacao", "subpav_altas_referenciadas", "monthly"),
    ("pacientes", "subpav_altas_referenciadas", "daily"),
    ("patologias_gestantes", "subpav_altas_referenciadas", "monthly"),
    ("protocolos_hemorragia", "subpav_altas_referenciadas", "monthly"),
    ("racas_cores", "subpav_altas_referenciadas", "monthly"),
    ("rn_intercorrencias", "subpav_altas_referenciadas", "daily"),
    ("sulfato_magnesio", "subpav_altas_referenciadas", "monthly"),
    ("suportes_ventilatorios", "subpav_altas_referenciadas", "monthly"),
    ("tipos_feedback", "subpav_altas_referenciadas", "monthly"),
    ("tipos_gravidez", "subpav_altas_referenciadas", "monthly"),
    ("vias_parto", "subpav_altas_referenciadas", "monthly"),
    # SISCegonha
    ("agendamento_gestantes", "subpav_cegonha", "daily"),
    ("agendamento_profissional", "subpav_cegonha", "monthly"),
    ("cadastro_gestante", "subpav_cegonha", "daily"),
    ("cadastro_gestante_log", "subpav_cegonha", "daily"),
    ("cegonha_feriados", "subpav_cegonha", "monthly"),
    ("cnes_dias_sem_visita", "subpav_cegonha", "monthly"),
    ("dados_maternidade_gestantes", "subpav_cegonha", "daily"),
    ("dias_sem_visita", "subpav_cegonha", "monthly"),
    ("gestacao_tipos", "subpav_cegonha", "monthly"),
    ("gestante_acompanhante", "subpav_cegonha", "monthly"),
    ("gestante_excecoes", "subpav_cegonha", "daily"),
    ("gestantes", "subpav_cegonha", "daily"),
    ("gestantes_videos_historico_login", "subpav_cegonha", "monthly"),
    ("gestantes_videos_unidades", "subpav_cegonha", "monthly"),
    ("horarios", "subpav_cegonha", "monthly"),
    ("maternidade_tipo_gestantes", "subpav_cegonha", "monthly"),
    ("maternidade_video", "subpav_cegonha", "monthly"),
    ("profissionais", "subpav_cegonha", "monthly"),
    ("raca_cor", "subpav_cegonha", "monthly"),
    ("semana_dias", "subpav_cegonha", "monthly"),
    ("situacao_excecoes", "subpav_cegonha", "monthly"),
    ("turnos", "subpav_cegonha", "monthly"),
    ("unidades_agendamento_vagas", "subpav_cegonha", "daily"),
    ("unidades_referencia_encaminha", "subpav_cegonha", "daily"),
    ("unidades_turnos_horarios", "subpav_cegonha", "daily"),
    ("1", "subpav_cegonha", "monthly"),
    ("visita_gestantes_tipos", "subpav_cegonha", "daily"),
    # Sinan Rio (Legado)
    ("tuberculose_sinan", "subpav_sinan", "daily"),
    ("tb_estabelecimento_saude", "subpav_sinan", "monthly"),
    # Sinan Rio (Novo)
    (
        "notificacao",
        "subpav_sinanrio",
        "daily",
        {
            "infisical_path": "/plataforma-subpav",
        },
    ),
    (
        "tb_investiga",
        "subpav_sinanrio",
        "daily",
        {
            "infisical_path": "/plataforma-subpav",
        },
    ),
    (
        "tb_sintomatico",
        "subpav_sinanrio",
        "daily",
        {
            "infisical_path": "/plataforma-subpav",
        },
    ),
]


def build_param(table_id, config, extra=None):
    """
    Gera um dicionário de parâmetros com base nas configurações por tabela.
    """
    default_params = {
        "table_id": table_id,
        "schema": config["schema"],
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
    }

    if extra:
        default_params.update(extra)
    return default_params


# -------- Agrupando por frequência
def unpack_params(freq):
    result = []
    for item in TABELAS_CONFIG:
        if item[2] != freq:
            continue
        table_id, schema = item[0], item[1]
        extra = item[3] if len(item) == 4 else None
        result.append(build_param(table_id, {"schema": schema}, extra=extra))
    return result


monthly_params = unpack_params("monthly")
weekly_params = unpack_params("weekly")
daily_params = unpack_params("daily")
per_hour_params = unpack_params("per_hour")

# Geração dos clocks (agendamentos) para cada grupo de tabelas com frequência específica.
# Os clocks são criados com base em um intervalo de tempo fixo (timedelta) e data de início.
monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=4),
    start_date=datetime(2025, 6, 1, 2, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=monthly_params,
    runs_interval_minutes=1,
)

weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 6, 1, 2, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=weekly_params,
    runs_interval_minutes=1,
)


daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 6, 1, 2, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=daily_params,
    runs_interval_minutes=1,
)

# Antes de usar por hora, verificar com o Pedro
per_hour_clocks = generate_dump_api_schedules(
    interval=timedelta(hours=1),
    start_date=datetime(2025, 6, 1, 2, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=per_hour_params,
    runs_interval_minutes=1,
)

# Instâncias de Schedule do Prefect que agrupam os clocks definidos acima.
# Podem ser usadas individualmente (mensal, semanal, por hora) ou em conjunto.
subpav_monthly_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))
subpav_weekly_schedule = Schedule(clocks=untuple_clocks(weekly_clocks))
subpav_daily_schedule = Schedule(clocks=untuple_clocks(daily_clocks))
subpav_per_hour_schedule = Schedule(clocks=untuple_clocks(per_hour_clocks))
subpav_combined_schedule = Schedule(
    clocks=untuple_clocks(daily_clocks + monthly_clocks + weekly_clocks + per_hour_clocks)
)
