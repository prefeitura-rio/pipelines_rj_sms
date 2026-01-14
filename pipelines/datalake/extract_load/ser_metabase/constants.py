# -*- coding: utf-8 -*-
"""
Constants
"""
from typing import Literal

DATABASE_IDS = {
    "DWH": {
        "id": 178,
        # Banco novo: Novas tabelas com diversos schemas que apresentam
        # aparentes melhoras nos dados disponibilizados.
        "tables": {
            # Lista dos recursos cadastrados
            "DIM_RECURSO": {"id": 3476, "slice_column": "recurso_id"},
            # Lista das unidades cadastradas
            "DIM_UNIDADE": {"id": 3477, "slice_column": "UNIDADE_ID"},
            # Solicitações ambulatoriais
            "FATO_AMBULATORIO": {
                "id": 3255,
                "slice_column": "solicitacao_id",
            },
            # Solicitações ambulatoriais
            "FATO_AMBULATORIO_2019": {
                "id": 5938,
                "slice_column": "solicitacao_id",
            },
            # Lista os eventos de todas as solicitações
            "FATO_HISTORICO_2019_ON": {
                "id": 5943,
                "slice_column": "solicitacao_id",
            },
            # Lista os eventos de todas as solicitações
            "FATO_HISTORICO_SOLICITACAO": {
                "id": 3260,
                "slice_column": "solicitacao_id",
            },
            # Solicitações de Internação
            "FATO_INTERNACAO": {
                "id": 3259,
                "slice_column": "solicitacao_id",
            },
            # (Descrição ausente)
            "FATO_INTERNACAO_2019": {
                "id": 5940,
                "slice_column": "solicitacao_id",
            },
            # (Descrição ausente)
            "Fato_Mapa_leitos": {
                "id": 3258,
                "slice_column": "",
            },
            # (Descrição ausente)
            "fato_fila_mensal": {"id": 5872, "slice_column": ""},
            # (Descrição ausente)
            "fato_fila_mensal_datalake": {"id": 5873, "slice_column": ""},
            # (Descrição ausente)
            "TB_RELATORIO_MEDICAMENTOS": {
                "id": 3478,
                "slice_column": "",
            },
            # (Descrição ausente)
            "TB_RELATORIO_DISPENSACAO_NOVO": {
                "id": 3479,
                "slice_column": "",
            },
        },
    },
    "REGULACAO_METRO1_CAPITAL": {
        "id": 173,
        # Banco com tabelas que estavam sendo utilizadas, mas apresentam
        # diversos problemas como inconsistência e falta de informações.
        "tables": {
            "TB_HISTORICO_SOLICITACAO_METRO1_CAPITAL": {"id": 3262, "slice_column": ""},
            "TB_QUANTIDADE_SOLICITACOES": {"id": 3261, "slice_column": ""},
            "TB_SOLICITACOES": {"id": 5783, "slice_column": "solicitacao_id"},
        },
    },
}


QUERY_COLUMNS: dict[
    int,
    dict[
        int,
        dict[
            Literal["slice_column", "slice_column_unique", "date_column", "slice_column_type"],
            int | str | bool,
        ],
    ],
] = {
    173: {
        # TB_SOLICITACOES
        5783: {
            "slice_column": 51407,
            "slice_column_unique": True,
            "slice_column_type": "String",
            "date_column": 51409,
        }
    },
    178: {
        # DIM_RECURSO
        3476: {
            "slice_column": 31534,
            "slice_column_unique": True,
            "slice_column_type": "Integer",
            "date_column": None,
        },
        # DIM_UNIDADE
        3477: {
            "slice_column": 31547,
            "slice_column_unique": True,
            "slice_column_type": "Integer",
            "date_column": None,
        },
        # FATO_AMBULATORIO
        3255: {
            "slice_column": 30429,
            "slice_column_unique": True,
            "slice_column_type": "String",
            "date_column": 30452,
        },
        # FATO_AMBULATORIO_2019
        5938: {
            "slice_column": 54371,
            "slice_column_unique": True,
            "slice_column_type": "Integer",
            "date_column": 54371,
        },
        # FATO_HISTORICO_2019_ON
        5943: {
            "slice_column": 54392,
            "slice_column_unique": False,
            "slice_column_type": "Integer",
            "date_column": 54386,
        },
        # FATO_HISTORICO_SOLICITACAO
        3260: {
            "slice_column": 30539,
            "slice_column_unique": False,
            "slice_column_type": "Integer",
            "date_column": 30545,
        },
        # FATO_INTERNACAO
        3259: {
            "slice_column": 30513,
            "slice_column_unique": True,
            "slice_column_type": "String",
            "date_column": 30522,
        },
        # FATO_INTERNACAO_2019
        5940: {
            "slice_column": 54424,
            "slice_column_unique": True,
            "slice_column_type": "Text",
            "date_column": 54394,
        },
        # Fato_Mapa_leitos
        3258: {
            "slice_column": 30479,
            "slice_column_unique": False,
            "slice_column_type": "Text",
            "date_column": 30474,
        },
        # fato_fila_mensal
        5872: {
            "slice_column": 52380,
            "slice_column_unique": False,
            "slice_column_type": "Integer",
            "date_column": 52416,
        },
        # fato_fila_mensal_datalake
        5873: {
            "slice_column": 52409,
            "slice_column_unique": False,
            "slice_column_type": "Text",
            "date_column": 52413,
        },
    },
}
"""
indexado por `database_id` e `table_id`, e tem como chaves:
- `slice_column`: o ID da coluna que será utilizada para o slice
- `slice_column_unique`: se a coluna é única ou não
- `date_column`: o ID da coluna de data, se existir
- `slice_column_type`: o tipo da coluna de slice, se necessário

Exemplo de uso:

database_id = 173
table_id = 5783

slice_column_id = QUERY_COLUMNS[database_id][table_id]['slice_column']
is_unique = QUERY_COLUMNS[database_id][table_id]['slice_column_unique']
date_column_id = QUERY_COLUMNS[database_id][table_id]['date_column']
slice_column_type = QUERY_COLUMNS[database_id][table_id]['slice_column_type']
"""
