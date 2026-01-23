# -*- coding: utf-8 -*-
# pylint: disable=import-error
"""
Schedules para o flow de migração do BigQuery para o MySQL da SUBPAV.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

RJ_TZ = pytz.timezone("America/Sao_Paulo")
LABELS = [constants.RJ_SMS_AGENT_LABEL.value]

TABLES_CONFIG = [
    {  # Sintomáticos Respiratórios
        "project": "SINANRIO - Sintomáticos respiratórios",
        "dataset_id": "projeto_sinanrio",
        "table_id": "sintomaticos_respiratorios_dia",
        "db_schema": "subpav_sinanrio",
        "dest_table": "tb_sintomatico",
        "frequency": "daily",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
        "custom_insert_query": """
        INSERT INTO tb_sintomatico (cpf, cns, nome, dt_nascimento, id_raca_cor, id_sexo, id_escolaridade, telefone,
        cep, logradouro, numero, complemento, id_bairro, cidade, cnes, ine, nao_municipe, n_prontuario, cnes_cadastrante,
        cpf_cadastrante, cns_cadastrante, id_tb_situacao, origem)
        SELECT :cpf, :cns, :nome, :dt_nascimento, :id_raca_cor, :id_sexo, :id_escolaridade, :telefone, :cep, :logradouro, :numero,
        :complemento, :id_bairro, :cidade, :cnes, :ine, :nao_municipe, :n_prontuario, IFNULL(:cnes_cadastrante, ''), IFNULL(:cpf_cadastrante, ''),
        IFNULL(:cns_cadastrante, ''), :id_tb_situacao, LEFT(:origem, 1)
        WHERE NOT EXISTS (
                SELECT 1
                FROM tb_sintomatico s
                WHERE (
                    (:cpf IS NOT NULL AND :cpf <> '' AND s.cpf = :cpf)
                    OR (:cns IS NOT NULL AND :cns <> '' AND s.cns = :cns)
                )
                AND s.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            )
        """,
    },
    {  # Notificações
        "project": "SINANRIO - Notificação",
        "dataset_id": "projeto_sinanrio",
        "table_id": "notificacao",
        "db_schema": "subpav_sinanrio",
        "dest_table": "notificacao",
        "frequency": "daily",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
        "custom_insert_query": """
            INSERT IGNORE INTO notificacao (
                nu_notificacao, dt_notificacao, co_cid, co_municipio_notificacao, co_unidade_notificacao, co_uf_notificacao,
                tp_notificacao, dt_diagnostico_sintoma, no_nome_paciente, dt_nascimento, nu_idade, tp_sexo,
                tp_gestante, tp_raca_cor, tp_escolaridade, nu_cartao_sus, no_nome_mae, co_uf_residencia,
                co_municipio_residencia, co_distrito_residencia, co_bairro_residencia, no_bairro_residencia, nu_cep_residencia, co_geo_campo_1,
                co_geo_campo_2, co_logradouro_residencia, no_logradouro_residencia, nu_residencia, ds_complemento_residencia, ds_referencia_residencia,
                nu_ddd_residencia, nu_telefone_residencia, tp_zona_residencia, co_pais_residencia, dt_investigacao, co_cbo_ocupacao,
                tp_classificacao_final, ds_classificacao_final, tp_criterio_confirmacao, tp_modo_infeccao, ds_modo_infeccao_outro, tp_local_infeccao,
                ds_local_infeccao_outro, tp_autoctone_residencia, co_uf_infeccao, co_pais_infeccao, co_municipio_infeccao, co_distrito_infeccao,
                co_bairro_infeccao, no_bairro_infeccao, co_unidade_infeccao, no_localidade_infeccao, st_doenca_trabalho, tp_evolucao_caso,
                ds_evolucao_caso_outro, dt_obito, dt_encerramento, ds_semana_notificacao, ds_semana_sintoma, ds_chave_fonetica,
                ds_soundex, dt_digitacao, dt_transf_us, dt_transf_dm, dt_transf_sm, dt_transf_rm,
                dt_transf_rs, dt_transf_se, nu_lote_vertical, nu_lote_horizontal, tp_duplicidade, tp_suspeita,
                st_vincula, tp_fluxo_retorno, st_modo_transmissao, tp_veiculo_transmissao, ds_veiculo_transmissao_outro, tp_local_surto,
                ds_local_outro, nu_caso_suspeito, tp_inquerito, nu_caso_examinado, nu_caso_positivo, ds_observacao,
                tp_delimitacao_surto, ds_delimitacao_surto_outro, st_fluxo_retorno_recebido, ds_identificador_registro, st_importado, st_criptografia,
                tp_sistema, st_espelho, ts_codigo1, ts_codigo2, tp_unidad_notificadora_externa, co_unidad_notificadora_externa,
                cpf_notificante, cpf_paciente, dnv_paciente, justificativa_cpf, lat, `long`,
                notif_assistente, resp_encerramento, st_agravo_tabagismo, st_pcr_escarro, tp_cultura_justificativa, visivel,
                finalizado, `timestamp`
            )
            SELECT
                :nu_notificacao, :dt_notificacao, :co_cid, :co_municipio_notificacao, :co_unidade_notificacao, :co_uf_notificacao,
                :tp_notificacao, :dt_diagnostico_sintoma, :no_nome_paciente, :dt_nascimento, :nu_idade, :tp_sexo,
                :tp_gestante, :tp_raca_cor, :tp_escolaridade, :nu_cartao_sus, :no_nome_mae, :co_uf_residencia,
                :co_municipio_residencia, :co_distrito_residencia, :co_bairro_residencia, :no_bairro_residencia, :nu_cep_residencia, :co_geo_campo_1,
                :co_geo_campo_2, :co_logradouro_residencia, :no_logradouro_residencia, :nu_residencia, :ds_complemento_residencia, :ds_referencia_residencia,
                :nu_ddd_residencia, :nu_telefone_residencia, :tp_zona_residencia, :co_pais_residencia, :dt_investigacao, :co_cbo_ocupacao,
                :tp_classificacao_final, :ds_classificacao_final, :tp_criterio_confirmacao, :tp_modo_infeccao, :ds_modo_infeccao_outro, :tp_local_infeccao,
                :ds_local_infeccao_outro, :tp_autoctone_residencia, :co_uf_infeccao, :co_pais_infeccao, :co_municipio_infeccao, :co_distrito_infeccao,
                :co_bairro_infeccao, :no_bairro_infeccao, :co_unidade_infeccao, :no_localidade_infeccao, :st_doenca_trabalho, :tp_evolucao_caso,
                :ds_evolucao_caso_outro, :dt_obito, :dt_encerramento, :ds_semana_notificacao, :ds_semana_sintoma, :ds_chave_fonetica,
                :ds_soundex, :dt_digitacao, :dt_transf_us, :dt_transf_dm, :dt_transf_sm, :dt_transf_rm,
                :dt_transf_rs, :dt_transf_se, :nu_lote_vertical, :nu_lote_horizontal, :tp_duplicidade, :tp_suspeita,
                :st_vincula, :tp_fluxo_retorno, :st_modo_transmissao, :tp_veiculo_transmissao, :ds_veiculo_transmissao_outro, :tp_local_surto,
                :ds_local_outro, :nu_caso_suspeito, :tp_inquerito, :nu_caso_examinado, :nu_caso_positivo, :ds_observacao,
                :tp_delimitacao_surto, :ds_delimitacao_surto_outro, :st_fluxo_retorno_recebido, :ds_identificador_registro, :st_importado, :st_criptografia,
                :tp_sistema, :st_espelho, :ts_codigo1, :ts_codigo2, :tp_unidad_notificadora_externa, :co_unidad_notificadora_externa,
                :cpf_notificante, :cpf_paciente, :dnv_paciente, :justificativa_cpf, :lat, :long,
                :notif_assistente, :resp_encerramento, :st_agravo_tabagismo, :st_pcr_escarro, :tp_cultura_justificativa, :visivel,
                :finalizado, :timestamp
        """,
    },
    {  # Investigações
        "project": "SINANRIO - Tabela de Investigação",
        "dataset_id": "projeto_sinanrio",
        "table_id": "tb_investiga",
        "db_schema": "subpav_sinanrio",
        "dest_table": "tb_investiga",
        "frequency": "daily",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
        "custom_insert_query": """
            INSERT IGNORE INTO tb_investiga (
                nu_notificacao, dt_notificacao, co_cid, co_municipio_notificacao, nu_prontuario, tp_entrada,
                tp_institucionalizado, tp_raio_x, tp_turberculinico, tp_forma, tp_extrapulmonar_1, tp_extrapulmonar_2,
                ds_extrapulmonar_outro, st_agravo_aids, st_agravo_alcolismo, st_agravo_diabete, st_agravo_mental, st_agravo_outro,
                ds_agravo_outro, st_baciloscopia_escarro, st_baciloscopia_outro, st_baciloscopia_escarro2, tp_cultura_escarro, tp_cultura_outro,
                tp_hiv, tp_histopatologia, dt_inicio_tratamento, st_droga_rifampicina, st_droga_isoniazida, st_droga_etambutol,
                st_droga_estreptomicina, st_droga_pirazinamida, st_droga_etionamida, st_droga_outro, ds_droga_outro, tp_tratamento,
                nu_contato, co_uf_atual, co_municipio_atual, nu_notificacao_atual, dt_notificacao_atual, co_unidade_saude_atual,
                st_baciloscopia_2_mes, st_baciloscopia_4_mes, st_baciloscopia_6_mes, st_baciloscopia_1_mes, st_baciloscopia_3_mes, st_baciloscopia_5_mes,
                tp_tratamento_acompanhamento, dt_mudanca_tratamento, nu_contato_examinado, tp_situacao_mes_9, tp_situacao_mes_12, tp_situacao_encerramento,
                co_uf_residencia_atual, co_municipio_residencia_atual, nu_cep_residencia_atual, co_distrito_residencia_atual, co_bairro_residencia_atual, no_bairro_residencia_atual,
                dt_encerramento, tp_pcr_escarro, tp_pop_liberdade, tp_pop_rua, tp_pop_saude, tp_pop_imigrante,
                tp_benef_gov, st_agravo_drogas, st_agravo_tabaco, tp_molecular, tp_sensibilidade, nu_contato_identificados,
                tp_antirretroviral_trat, st_bacil_apos_6_mes, nu_prontuario_atual, tp_transf, co_uf_transf, co_municipio_transf
            )
            SELECT
                :nu_notificacao, :dt_notificacao, :co_cid, :co_municipio_notificacao, :nu_prontuario, :tp_entrada,
                :tp_institucionalizado, :tp_raio_x, :tp_turberculinico, :tp_forma, :tp_extrapulmonar_1, :tp_extrapulmonar_2,
                :ds_extrapulmonar_outro, :st_agravo_aids, :st_agravo_alcolismo, :st_agravo_diabete, :st_agravo_mental, :st_agravo_outro,
                :ds_agravo_outro, :st_baciloscopia_escarro, :st_baciloscopia_outro, :st_baciloscopia_escarro2, :tp_cultura_escarro, :tp_cultura_outro,
                :tp_hiv, :tp_histopatologia, :dt_inicio_tratamento, :st_droga_rifampicina, :st_droga_isoniazida, :st_droga_etambutol,
                :st_droga_estreptomicina, :st_droga_pirazinamida, :st_droga_etionamida, :st_droga_outro, :ds_droga_outro, :tp_tratamento,
                :nu_contato, :co_uf_atual, :co_municipio_atual, :nu_notificacao_atual, :dt_notificacao_atual, :co_unidade_saude_atual,
                :st_baciloscopia_2_mes, :st_baciloscopia_4_mes, :st_baciloscopia_6_mes, :st_baciloscopia_1_mes, :st_baciloscopia_3_mes, :st_baciloscopia_5_mes,
                :tp_tratamento_acompanhamento, :dt_mudanca_tratamento, :nu_contato_examinado, :tp_situacao_mes_9, :tp_situacao_mes_12, :tp_situacao_encerramento,
                :co_uf_residencia_atual, :co_municipio_residencia_atual, :nu_cep_residencia_atual, :co_distrito_residencia_atual, :co_bairro_residencia_atual, :no_bairro_residencia_atual,
                :dt_encerramento, :tp_pcr_escarro, :tp_pop_liberdade, :tp_pop_rua, :tp_pop_saude, :tp_pop_imigrante,
                :tp_benef_gov, :st_agravo_drogas, :st_agravo_tabaco, :tp_molecular, :tp_sensibilidade, :nu_contato_identificados,
                :tp_antirretroviral_trat, :st_bacil_apos_6_mes, :nu_prontuario_atual, :tp_transf, :co_uf_transf, :co_municipio_transf
        """,
    },
    {  # Resultados de Exames (todos)
        "project": "SINANRIO - Resultado de Exames",
        "dataset_id": "projeto_sinanrio",
        "table_id": "resultado_exame",
        "db_schema": "subpav_sinanrio",
        "dest_table": "tb_resultado_exame",
        "frequency": "daily",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
        "custom_insert_query": """
            INSERT INTO tb_resultado_exame (
                codigo_amostra, paciente_cpf, paciente_cns, cnes, id_tipo_exame, id_resultado,
                dt_resultado, notificacao_ativa, diagnostico, updated_at
            )
            VALUES (
                :codigo_amostra, :paciente_cpf, :paciente_cns, :cnes, :id_tipo_exame, :id_resultado,
                :dt_resultado, :notificacao_ativa, :diagnostico, CURRENT_TIMESTAMP
            )
            ON DUPLICATE KEY UPDATE
                paciente_cpf        = VALUES(paciente_cpf),
                cnes                = VALUES(cnes),
                id_resultado        = VALUES(id_resultado),
                notificacao_ativa   = VALUES(notificacao_ativa),
                diagnostico         = VALUES(diagnostico),
                updated_at          = CURRENT_TIMESTAMP
        """,
    },
    {  # Resultados de Exames (Atualização de sintomatico)
        "project": "SINANRIO - Atualização de Exames Sintomaticos",
        "dataset_id": "projeto_sinanrio",
        "table_id": "resultado_exame",
        "db_schema": "subpav_sinanrio",
        "dest_table": "tb_sintomatico",
        "frequency": "daily",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
        "custom_insert_query": """
            UPDATE tb_sintomatico s
            SET
                s.id_trmtb = CASE
                    WHEN :id_tipo_exame = 1
                    AND (:dt_resultado >= IFNULL(s.dt_trmtb, '1000-01-01') OR s.id_trmtb IS NULL)
                    THEN :id_resultado
                    ELSE s.id_trmtb
                END,

                s.dt_trmtb = CASE
                    WHEN :id_tipo_exame = 1
                    AND (:dt_resultado >= IFNULL(s.dt_trmtb, '1000-01-01') OR s.dt_trmtb IS NULL)
                    THEN :dt_resultado
                    ELSE s.dt_trmtb
                END,

                s.id_bac_1 = CASE
                    WHEN :id_tipo_exame = 2
                    AND s.id_bac_1 IS NULL
                    THEN :id_resultado
                    ELSE s.id_bac_1
                END,

                s.dt_bac_1 = CASE
                    WHEN :id_tipo_exame = 2
                    AND s.dt_bac_1 IS NULL
                    THEN :dt_resultado
                    ELSE s.dt_bac_1
                END,

                s.id_bac_2 = CASE
                    WHEN :id_tipo_exame = 2
                    AND s.id_bac_1 IS NOT NULL
                    AND s.id_bac_2 IS NULL
                    THEN :id_resultado
                    WHEN :id_tipo_exame = 2
                    AND s.id_bac_2 IS NOT NULL
                    AND :dt_resultado >= IFNULL(s.dt_bac_2, '1000-01-01')
                    THEN :id_resultado
                    ELSE s.id_bac_2
                END,

                s.dt_bac_2 = CASE
                    WHEN :id_tipo_exame = 2
                    AND s.id_bac_1 IS NOT NULL
                    AND s.dt_bac_2 IS NULL
                    THEN :dt_resultado
                    WHEN :id_tipo_exame = 2
                    AND s.dt_bac_2 IS NOT NULL
                    AND :dt_resultado >= IFNULL(s.dt_bac_2, '1000-01-01')
                    THEN :dt_resultado
                    ELSE s.dt_bac_2
                END

            WHERE :diagnostico = 1
            AND s.id_tb_situacao IN (1, 2)
            AND (
                    ( :paciente_cns IS NOT NULL AND :paciente_cns <> '' AND s.cns = :paciente_cns )
                OR ( :paciente_cpf IS NOT NULL AND :paciente_cpf <> '' AND s.cpf = :paciente_cpf )
            )
        """,
    },
]


def build_param(config: dict) -> dict:
    param = {
        "dataset_id": config["dataset_id"],
        "table_id": config["table_id"],
        "db_schema": config["db_schema"],
        "dest_table": config["dest_table"],
        "infisical_path": config["infisical_path"],
        "rename_flow": True,
        "if_exists": config.get("if_exists", "append"),
        "project": config.get("project", ""),
        "environment": config.get("environment", "dev"),
    }
    # Só adiciona se tiver valor!
    if config.get("secret_name") is not None:
        param["secret_name"] = config["secret_name"]
    if config.get("bq_columns") is not None:
        param["bq_columns"] = config["bq_columns"]
    if config.get("custom_insert_query") is not None:
        param["custom_insert_query"] = config["custom_insert_query"]
    if config.get("limit") is not None:
        param["limit"] = config["limit"]
    if config.get("notify") is not None:
        param["notify"] = config["notify"]
    return param


def unpack_params(frequency: str) -> list:
    """
    Retorna uma lista de parâmetros prontos para execução do flow,
    filtrando as configurações de tabela pela frequência informada.
    """
    return [build_param(config) for config in TABLES_CONFIG if config["frequency"] == frequency]


daily_params = unpack_params("daily")
weekly_params = unpack_params("weekly")
monthly_params = unpack_params("monthly")

BASE_START_DATE = datetime(2025, 7, 31, 7, 0, tzinfo=RJ_TZ)

daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=daily_params,
)

weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=weekly_params,
    runs_interval_minutes=1,
)

monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=4),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=monthly_params,
    runs_interval_minutes=1,
)

bq_to_subpav_daily_schedule = Schedule(clocks=untuple_clocks(daily_clocks))
bq_to_subpav_weekly_schedule = Schedule(clocks=untuple_clocks(weekly_clocks))
bq_to_subpav_monthly_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))
bq_to_subpav_combined_schedule = Schedule(
    clocks=untuple_clocks(daily_clocks + weekly_clocks + monthly_clocks)
)
