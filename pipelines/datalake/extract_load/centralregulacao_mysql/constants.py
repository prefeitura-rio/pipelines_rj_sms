# -*- coding: utf-8 -*-
"""
Constantes
"""

SCHEMAS = {
    "monitoramento": {
        "host": "db.smsrio.org",
        "port": None,
        "tables": [
            "vw_MS_CadastrosAtivacoesGov",
            "vw_fibromialgia_relatorio",
            "vw_minhaSaude_listaUsuario",
            "vw_tea_relatorio",
        ],
    },
    "dw": {
        "host": "db.smsrio.org",
        "port": None,
        "tables": [
            "vw_minhasauderio_pesquisa_satisfacao"
        ],
    },
}
