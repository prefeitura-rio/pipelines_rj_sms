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
        "source_table_id": "rj-smfp.recursos_humanos_ergon_saude.funcionarios_ativos",
        "destination_dataset_name": "brutos_ergon_staging",
        "destination_table_name": "funcionarios_ativos",
        "environment": "prod",
        "dbt_select_exp": "tag:ergon",
    },
    {
        "source_table_id": "rj-smfp.compras_materiais_servicos_sigma_staging.material",
        "destination_dataset_name": "brutos_sigma_staging",
        "destination_table_name": "material",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-smfp.compras_materiais_servicos_sigma_staging.grupo",
        "destination_dataset_name": "brutos_sigma_staging",
        "destination_table_name": "grupo",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-smfp.compras_materiais_servicos_sigma_staging.classe",
        "destination_dataset_name": "brutos_sigma_staging",
        "destination_table_name": "classe",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-smfp.compras_materiais_servicos_sigma_staging.subclasse",
        "destination_dataset_name": "brutos_sigma_staging",
        "destination_table_name": "subclasse",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.administracao_perfil",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "administracao_perfil",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.administracao_unidade",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "administracao_unidade",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.administracao_unidade_perfil",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "administracao_unidade_perfil",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.agencia",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "agencia",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.banco",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "banco",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.conta_bancaria",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "conta_bancaria",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.conta_bancaria_tipo",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "conta_bancaria_tipo",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.contrato",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "contrato",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.contrato_terceiros",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "contrato_terceiros",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.despesas",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "despesas",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.estado_entrega",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "estado_entrega",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.fechamento",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "fechamento",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.fornecedor",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "fornecedor",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.historico_alteracoes",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "historico_alteracoes",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.itens_nota_fiscal",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "itens_nota_fiscal",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.receita_dados",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "receita_dados",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.receita_item",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "receita_item",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.rubrica",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "rubrica",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.saldo_dados",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "saldo_dados",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.saldo_item",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "saldo_item",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.secretaria",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "secretaria",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.tipo_arquivo",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "tipo_arquivo",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.tipo_documento",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "tipo_documento",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.usuario",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "usuario",
        "environment": "prod",
    },
    {
        "source_table_id": "rj-cvl.adm_contrato_gestao.usuario_sistema",
        "destination_dataset_name": "brutos_osinfo_staging",
        "destination_table_name": "usuario_sistema",
        "environment": "prod",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
