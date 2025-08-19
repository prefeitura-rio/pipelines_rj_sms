# -*- coding: utf-8 -*-
"""
Constants
"""
from typing import Literal

DATABASE_IDS = {
    "DWH": {
        "id": 178,  # Banco novo: Novas tabelas com diversos schemas que apresentam aparentes
        # melhoras nos dados disponibilizados.
        "tables": {
            "DIM_RECURSO": {"id": 3476, "slice_column": ""},  # Lista dos recursos cadastrados
            "DIM_UNIDADE": {"id": 3477, "slice_column": ""},  # Lista das unidades cadastradas
            "FATO_AMBULATORIO": {
                "id": 3255,  # Solicitações ambulatoriais
                "slice_column": "solicitacao_id",
            },
            "FATO_HISTORICO_SOLICITACAO": {
                "id": 3260,  # Lista os eventos de todas as solicitações
                "slice_column": "solicitacao_id",
            },
            "FATO_INTERNACAO": {
                "id": 3259,  # Solicitações de Internação
                "slice_column": "solicitacao_id",
            },
            "FATO_MAPA_LEITOS": {"id": 3258, "slice_column": ""},  # (Descrição ausente)
            "FATO_FILA_MENSAL": {"id": 5872, "slice_column": ""},  # (Descrição ausente)
            "FATO_FILA_MENSAL_DATALAKE": {"id": 5873, "slice_column": ""},  # (Descrição ausente)
            "TB_RELATORIO_MEDICAMENTOS": {
                "id": 3478,  # (Descrição ausente)
                "slice_column": "",
            },
            "TB_RELATORIO_DISPENSACAO_NOVO": {
                "id": 3479,  # (Descrição ausente)
                "slice_column": "",
            },
        },
    },
    "REGULACAO_METRO1_CAPITAL": {
        "id": 173,  # Banco com tabelas que estavam sendo utilizadas, mas apresentam
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
            Literal["slice_column", "slice_column_unique", "date_column", "slice_column_type"], any
        ],
    ],
] = {
    173: {
        5783: {
            "slice_column": 51407,
            "slice_column_unique": True,
            "date_column": 51409,
            "slice_column_type": "String",
        }
    },
    178: {
        3255: {
            "slice_column": 30429,
            "slice_column_unique": True,
            "date_column": 30452,
            "slice_column_type": "String",
        },
        3259: {
            "slice_column": 30513,
            "slice_column_unique": True,
            "date_column": 30522,
            "slice_column_type": "String",
        },
        3260: {
            "slice_column": 30539,
            "slice_column_unique": False,
            "date_column": 30545,
            "slice_column_type": "Integer",
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
