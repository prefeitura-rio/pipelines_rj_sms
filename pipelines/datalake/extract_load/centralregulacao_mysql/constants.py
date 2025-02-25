# -*- coding: utf-8 -*-
"""
Constantes
"""

SCHEMAS = { 
    
    "monitoramento" : 
    {
        "host" : "db.smsrio.org",
        "port" : None,
        "tables" : [
            "vw_MS_CadastrosAtivacoesGov",
            "vw_fibromialgia_relatorio",
            "vw_minhaSaude_listaUsuario",
            "vw_tea_relatorio"
        ]

    },

    "dw" : 
    {
        "host" : "dev.smsrio.org",
        "port" : None,
        "tables" : [
            "tb_execucao",
            "tb_execucao_agrupado",
            "tb_lista_autorizados",
            "tb_lista_solicitacoes_1vez",
            "tb_solicitacoes_1vez_agrupado"
        ]

    },
}








