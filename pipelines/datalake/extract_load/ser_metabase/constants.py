# -*- coding: utf-8 -*-
"""
Constants
"""
DATABASE_IDS = {
    "DWH": {
        "id": 178,  # Banco novo: Novas tabelas com diversos schemas que apresentam aparentes
        # melhoras nos dados disponibilizados.
        "tables": {
            "DIM_RECURSO": {
                "id": 3476,  # Lista dos recursos cadastrados
                "slice_column": ""
            },
            "DIM_UNIDADE": {
                "id": 3477,  # Lista das unidades cadastradas
                "slice_column": ""
            },
            "FATO_AMBULATORIO": {
                "id": 3255,  # Solicitações ambulatoriais
                "slice_column": "solicitacao_id"
            },
            "FATO_HISTORICO_SOLICITACAO": {
                "id": 3260,  # Lista os eventos de todas as solicitações
                "slice_column": ""
            },
            "FATO_INTERNACAO": {
                "id": 3259,  # Solicitações de Internação
                "slice_column": ""
            },
            "FATO_MAPA_LEITOS": {
                "id": 3258,  # (Descrição ausente)
                "slice_column": ""
            },
            "FATO_FILA_MENSAL": {
                "id": 5872,  # (Descrição ausente)
                "slice_column": ""
            },
            "FATO_FILA_MENSAL_DATALAKE": {
                "id": 5873,  # (Descrição ausente)
                "slice_column": ""
            },
            "TB_RELATORIO_MEDICAMENTOS": {
                "id": 3478,  # (Descrição ausente)
                "slice_column": ""
            },
            "TB_RELATORIO_DISPENSACAO_NOVO": {
                "id": 3479,  # (Descrição ausente)
                "slice_column": ""
            },
        },
    },
    "REGULACAO_METRO1_CAPITAL": {
        "id": 173,  # Banco com tabelas que estavam sendo utilizadas, mas apresentam
        # diversos problemas como inconsistência e falta de informações.
        "tables": {
            "TB_HISTORICO_SOLICITACAO_METRO1_CAPITAL": {
                "id": 3262,
                "slice_column": ""
            },
            "TB_QUANTIDADE_SOLICITACOES": {
                "id": 3261,
                "slice_column": ""
            },
            "TB_SOLICITACOES": {
                "id": 5783,
                "slice_column": "solicitacao_id"
            },
        },
    },
}


SLICE_COLUMNS: dict[int, dict[int, int]] = {
    173: {
        5783: 51407,
    },
    178: {
        3255: 30429,
        3259: 30513,
    }
}
"""
dict[int, dict[int, int]]

indexado por `database_id` e `table_id`

uso:
    database_id = 173
    table_id = 5783

    column_id = SLICE_COLUMNS[database_id][table_id]
"""
