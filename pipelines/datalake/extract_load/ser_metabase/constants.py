# -*- coding: utf-8 -*-
DATABASE_IDS = {
    "DWH": {
        "id": 178,  # Banco novo: Novas tabelas com diversos schemas que apresentam aparentes
                    # melhoras nos dados disponibilizados.
        "tables": {
            "DIM_RECURSO": 3476,  # Lista dos recursos cadastrados
            "DIM_UNIDADE": 3477,  # Lista das unidades cadastradas
            "FATO_AMBULATORIO": 3255,  # Solicitações ambulatoriais
            "FATO_HISTORICO_SOLICITACAO": 3260,  # Lista os eventos de todas as solicitações
            "FATO_INTERNACAO": 3259,  # Solicitações de Internação
            "FATO_MAPA_LEITOS": 3258,  # (Descrição ausente)
            "FATO_FILA_MENSAL": 5872,  # (Descrição ausente)
            "FATO_FILA_MENSAL_DATALAKE": 5873,  # (Descrição ausente)
            "TB_RELATORIO_MEDICAMENTOS": 3778,  # (Descrição ausente)
            "TB_RELATORIO_DISPENSACAO_NOVO": 3479,  # (Descrição ausente)
        },
    },
    "REGULACAO_METRO1_CAPITAL": {
        "id": 173,  # Banco com tabelas que estavam sendo utilizadas, mas apresentam
                    # diversos problemas como inconsistência e falta de informações.
        "tables": {
            "TB_HISTORICO_SOLICITACAO_METRO1_CAPITAL": 3262,
            "TB_QUANTIDADE_SOLICITACOES": 3261,
            "TB_SOLICITACOES": 5261,
        },
    },
}
