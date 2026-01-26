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
    ("indicadores", "subpav_indicadores", "monthly"),
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
    ("altas", "subpav_altas_referenciadas", "daily", {"id_column": "id_alta"}),
    (
        "altas_arquivos",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_alta_arquivo"},
    ),
    (
        "altas_medicamentos",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_alta_medicamento"},
    ),
    (
        "altas_pendentes",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_alta_pendente"},
    ),
    (
        "altas_pendentes_referenciadas",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_alta_pendente_referenciada"},
    ),
    ("apgars", "subpav_altas_referenciadas", "monthly", {"id_column": "id_apgar"}),
    (
        "comorbidades",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_comorbidade"},
    ),
    ("dashboard", "subpav_altas_referenciadas", "monthly", {"id_column": "visualizacao"}),
    ("dashboard_feedback", "subpav_altas_referenciadas", "monthly"),
    (
        "desfechos_gestacao",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_desfecho_gestacao"},
    ),
    (
        "desfechos_internacao",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_desfecho_internacao"},
    ),
    ("exames", "subpav_altas_referenciadas", "monthly", {"id_column": "id_exame"}),
    ("feedbacks", "subpav_altas_referenciadas", "daily", {"id_column": "id_feedback"}),
    (
        "feridas_cirurgicas",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_ferida_cirurgica"},
    ),
    (
        "formas_entrada",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_forma_entrada"},
    ),
    (
        "formularios",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_formulario"},
    ),
    (
        "formularios_exames",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_formulario_exame"},
    ),
    (
        "formularios_motivos_internacao",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_formulario_motivo_internacao"},
    ),
    ("gestantes", "subpav_altas_referenciadas", "daily", {"id_column": "id_gestante"}),
    (
        "gestantes_patologias",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_gestante_patologia"},
    ),
    (
        "gestantes_rn",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_gestante_rn"},
    ),
    (
        "gestantes_uti",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_gestante_uti"},
    ),
    (
        "gestantes_uti_hemoderivados",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_gestante_uti_hemoderivado"},
    ),
    (
        "gestantes_uti_hemorragias",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_gestante_uti_hemorragia"},
    ),
    (
        "gestantes_uti_ventilatorios",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_gestante_uti_ventilatorio"},
    ),
    (
        "hemoderivados",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_hemoderivado"},
    ),
    (
        "histerectomia_causas",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_histerectomia_causa"},
    ),
    (
        "histerectomia_tipos",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_histerectomia_tipo"},
    ),
    (
        "historico_edicao",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_historico_edicao"},
    ),
    (
        "intercorrencias",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_intercorrencia"},
    ),
    (
        "internacoes",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_internacao"},
    ),
    (
        "internacoes_comorbidades",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_internacao_comorbidade"},
    ),
    (
        "internacoes_diagnosticos",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_internacao_diagnostico"},
    ),
    (
        "internacoes_exames",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_internacao_exame"},
    ),
    (
        "internacoes_intercorrencias",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_internacao_intercorrencia"},
    ),
    ("log", "subpav_altas_referenciadas", "daily", {"id_column": "id_log"}),
    ("log_dashboard", "subpav_altas_referenciadas", "daily"),
    (
        "log_tabelas",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_log_tabela"},
    ),
    (
        "motivos_alta",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_motivo_alta"},
    ),
    (
        "motivos_clinicos",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_motivo_clinico"},
    ),
    (
        "motivos_internacao",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_motivo_internacao"},
    ),
    ("pacientes", "subpav_altas_referenciadas", "daily", {"id_column": "id_paciente"}),
    (
        "patologias_gestantes",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_patologia_gestante"},
    ),
    (
        "protocolos_hemorragia",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_protocolo_hemorragia"},
    ),
    (
        "racas_cores",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_raca_cor"},
    ),
    (
        "rn_intercorrencias",
        "subpav_altas_referenciadas",
        "daily",
        {"id_column": "id_rn_intercorrencia"},
    ),
    (
        "sulfato_magnesio",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_sulfato_magnesio"},
    ),
    (
        "suportes_ventilatorios",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_suporte_ventilatorio"},
    ),
    (
        "tipos_feedback",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_tipo_feedback"},
    ),
    (
        "tipos_gravidez",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_tipo_gravidez"},
    ),
    (
        "vias_parto",
        "subpav_altas_referenciadas",
        "monthly",
        {"id_column": "id_via_parto"},
    ),
    (
        "agendamento_gestantes",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_agendamento_gestante"},
    ),
    (
        "agendamento_profissional",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_agendamento_profissional"},
    ),
    ("cadastro_gestante", "subpav_cegonha", "daily"),
    ("cadastro_gestante_log", "subpav_cegonha", "daily"),
    ("cegonha_feriados", "subpav_cegonha", "monthly", {"id_column": "id_feriado"}),
    (
        "cnes_dias_sem_visita",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_cnes_dias_sem_visita"},
    ),
    (
        "dados_maternidade_gestantes",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_dados_maternidade_gestante"},
    ),
    (
        "dias_sem_visita",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_dias_sem_visita"},
    ),
    (
        "gestacao_tipos",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_gestacao_tipo"},
    ),
    (
        "gestante_acompanhante",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_acompanhante"},
    ),
    (
        "gestante_excecoes",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_gestante_excecao"},
    ),
    ("gestantes", "subpav_cegonha", "daily", {"id_column": "id_gestante"}),
    ("gestantes_videos_historico_login", "subpav_cegonha", "monthly"),
    ("gestantes_videos_unidades", "subpav_cegonha", "monthly"),
    ("horarios", "subpav_cegonha", "monthly", {"id_column": "id_horario"}),
    (
        "maternidade_tipo_gestantes",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_maternidade_tipos_gestante"},
    ),
    ("maternidade_video", "subpav_cegonha", "monthly"),
    ("profissionais", "subpav_cegonha", "monthly", {"id_column": "id_profissionais"}),
    ("raca_cor", "subpav_cegonha", "monthly"),
    ("semana_dias", "subpav_cegonha", "monthly", {"id_column": "id_semana_dia"}),
    (
        "situacao_excecoes",
        "subpav_cegonha",
        "monthly",
        {"id_column": "id_situacao_excecao"},
    ),
    ("turnos", "subpav_cegonha", "monthly", {"id_column": "id_turno"}),
    (
        "unidades_agendamento_vagas",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_unidades_agendamento_vagas"},
    ),
    (
        "unidades_referencia_encaminha",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_unidades_referencia_encaminha"},
    ),
    (
        "unidades_turnos_horarios",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_turnos_horario"},
    ),
    ("unidades_videos", "subpav_cegonha", "monthly", {"id_column": "id_unidade_video"}),
    (
        "visita_gestantes_tipos",
        "subpav_cegonha",
        "daily",
        {"id_column": "id_visita_gestante_tipo"},
    ),
    ("tuberculose_sinan", "subpav_sinan", "daily"),
    ("tb_estabelecimento_saude", "subpav_sinan", "monthly"),
    ("notificacao", "subpav_sinanrio", "daily", {"infisical_path": "/plataforma-subpav"}),
    ("tb_investiga", "subpav_sinanrio", "daily", {"infisical_path": "/plataforma-subpav"}),
    ("tb_sintomatico", "subpav_sinanrio", "daily", {"infisical_path": "/plataforma-subpav"}),
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


def unpack_params(freq):
    """
    Agrupando por frequência
    """
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
