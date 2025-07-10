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
        "slug": "cnes__ftp",
        "source_project_name": "basedosdados",
        "source_dataset_name": "br_ms_cnes",
        "source_table_list": [
            "estabelecimento",
            "equipamento",
            "habilitacao",
            "leito",
            "profissional",
        ],
        "destination_dataset_name": "brutos_cnes_ftp_staging",
        "environment": "prod",
    },
    {
        "slug": "osinfo",
        "source_project_name": "rj-cvl",
        "source_dataset_name": "adm_contrato_gestao",
        "source_table_list": [
            "administracao_perfil",
            "administracao_unidade",
            "agencia",
            "banco",
            "conta_bancaria",
            "conta_bancaria_tipo",
            "contrato",
            "contrato_terceiros",
            "despesas",
            "estado_entrega",
            "fechamento",
            "fornecedor",
            "historico_alteracoes",
            "itens_nota_fiscal",
            "plano_contas",
            "receita_dados",
            "receita_item",
            "rubrica",
            "saldo_dados",
            "saldo_item",
            "secretaria",
            "tipo_arquivo",
            "tipo_documento",
            "usuario",
            "usuario_sistema",
        ],
        "destination_dataset_name": "brutos_osinfo_staging",
        "environment": "prod",
    },
    {
        "slug": "ergon",
        "source_project_name": "rj-smfp",
        "source_dataset_name": "recursos_humanos_ergon_saude",
        "source_table_list": [
            "funcionarios",
        ],
        "destination_dataset_name": "brutos_ergon_staging",
        "environment": "prod",
        "dbt_select_exp": "tag:ergon",
    },
    {
        "slug": "sigma",
        "source_project_name": "rj-smfp",
        "source_dataset_name": "compras_materiais_servicos_sigma_staging",
        "source_table_list": ["classe", "grupo", "material", "subclasse"],
        "destination_dataset_name": "brutos_sigma_staging",
        "environment": "prod",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 00, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
