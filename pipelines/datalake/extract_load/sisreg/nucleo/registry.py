# -*- coding: utf-8 -*-
"""
Registry de conjuntos de dados do SISREG.

Define o dataclass ConjuntoSisreg e o dicionario CONJUNTOS com os 5 datasets
do fluxo unificado. Cada conjunto declara apenas o que o diferencia dos demais:
perfil de credencial, tabelas de destino, colunas esperadas e as funcoes
planejar_trabalho/extrair_item do seu extrator.

A logica generica (autenticacao, politeness, consolidacao, upload, log) fica
em tasks.py, auth.py, http.py e parsing.py. Nenhum conjunto reimplementa isso.

Os extratores nao importam este modulo (dependem apenas de constants, errors,
resultado, auth, http e parsing); o registry importa os extratores e liga as
funcoes diretamente - sem auto-registro nem imports tardios.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, Tuple

from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.nucleo.extractors import (
    afastamentos,
    escalas,
    fila_vagas,
    preparos,
    solicitacoes,
)


@dataclass(frozen=True)
class ConjuntoSisreg:
    """Configuracao imutavel de um conjunto de dados do SISREG.

    'colunas_esperadas' pode mapear para frozenset vazio quando o schema e
    confiado ao extrator (ex.: escalas, cujo CSV e a fonte de verdade).
    """

    # Identificador unico do conjunto (chave no dicionario CONJUNTOS).
    chave: str

    # Perfil de credencial a usar: "padrao" (/sisreg) ou "regulador" (/sisreg_regulacao).
    # Deve ser uma chave valida em constants.PERFIS_CREDENCIAL.
    perfil_credencial: str

    # Nomes das tabelas de destino no datalake (1 ou 2 por conjunto).
    tabelas: Tuple[str, ...]

    # Funcao que determina o item de trabalho (plano) do conjunto.
    # Cada conjunto produz UM unico item por execucao (single-flight, uma
    # sessao por run); o loop sobre sub-itens fica dentro do extrator.
    # Assinatura: (credenciais: dict, params: dict) -> dict
    planejar_trabalho: Callable

    # Funcao que extrai o item e retorna os dados por tabela.
    # Assinatura: (sessao, item: dict, params: dict) -> ResultadoConjunto | dict
    extrair_item: Callable

    # Colunas esperadas por tabela: {nome_tabela: frozenset(colunas)}.
    # Usadas em consolidar() para validar schema e detectar deriva silenciosa.
    colunas_esperadas: Dict[str, FrozenSet[str]] = field(default_factory=dict)

    # Modo de escrita no datalake: "overwrite" (padrao - substitui a tabela a
    # cada execucao 100% completa) ou "append" (acrescenta as linhas do run).
    # preparos usa "append" porque e paginado por shard (cada run cobre so um
    # subconjunto de unidades); a deduplicacao "ultima por unidade" e resolvida
    # na camada de modelagem. Nunca use overwrite com um extrator paginado - isso
    # apagaria os shards das execucoes anteriores.
    modo_escrita: str = "overwrite"


# Dicionario principal: chave -> ConjuntoSisreg.
CONJUNTOS: Dict[str, ConjuntoSisreg] = {
    "escalas": ConjuntoSisreg(
        chave="escalas",
        perfil_credencial="padrao",
        tabelas=(C.TABELA_ESCALAS,),
        planejar_trabalho=escalas.planejar_trabalho_escalas,
        extrair_item=escalas.extrair_item_escalas,
        colunas_esperadas={
            # Schema confiado: o CSV exportado e a fonte de verdade (34 colunas).
            C.TABELA_ESCALAS: frozenset(),
        },
    ),
    "afastamentos": ConjuntoSisreg(
        chave="afastamentos",
        perfil_credencial="regulador",
        tabelas=(C.TABELA_AFASTAMENTOS, C.TABELA_AFASTAMENTO_HISTORICO),
        planejar_trabalho=afastamentos.planejar_trabalho_afastamentos,
        extrair_item=afastamentos.extrair_item_afastamentos,
        colunas_esperadas={
            C.TABELA_AFASTAMENTOS: frozenset(
                ["cpf", "data_inicio", "data_fim", "hora_inicio", "hora_fim", C.COLUNA_PARTICAO]
            ),
            C.TABELA_AFASTAMENTO_HISTORICO: frozenset(["cpf", "data", C.COLUNA_PARTICAO]),
        },
    ),
    "preparos": ConjuntoSisreg(
        chave="preparos",
        perfil_credencial="padrao",
        tabelas=(C.TABELA_PREPAROS,),
        planejar_trabalho=preparos.planejar_trabalho_preparos,
        extrair_item=preparos.extrair_item_preparos,
        colunas_esperadas={
            C.TABELA_PREPAROS: frozenset(
                [
                    "cnes",
                    "nome_unidade",
                    "cod_procedimento",
                    "nome_procedimento",
                    "descricao_preparo",
                ]
            ),
        },
        # Paginado por shard de unidades -> append (nao overwrite). Ver preparos.py.
        modo_escrita="append",
    ),
    "solicitacoes": ConjuntoSisreg(
        chave="solicitacoes",
        perfil_credencial="padrao",
        tabelas=(C.TABELA_SOLICITACOES,),
        planejar_trabalho=solicitacoes.planejar_trabalho_solicitacoes,
        extrair_item=solicitacoes.extrair_item_solicitacoes,
        colunas_esperadas={
            C.TABELA_SOLICITACOES: frozenset(),
        },
    ),
    "fila_vagas": ConjuntoSisreg(
        chave="fila_vagas",
        perfil_credencial="regulador",
        tabelas=(C.TABELA_FILA_E_VAGAS, C.TABELA_VAGAS_DETALHADAS),
        planejar_trabalho=fila_vagas.planejar_trabalho_fila_vagas,
        extrair_item=fila_vagas.extrair_item_fila_vagas,
        colunas_esperadas={
            C.TABELA_FILA_E_VAGAS: frozenset(
                ["cod_interno_proced", "cnes_unidade", "qtd_pend", "qtd_vagas", C.COLUNA_PARTICAO]
            ),
            C.TABELA_VAGAS_DETALHADAS: frozenset(
                ["cod_interno_proced", "cnes_unidade", "data_vaga", C.COLUNA_PARTICAO]
            ),
        },
    ),
}


def obter_conjunto(chave: str) -> ConjuntoSisreg:
    """Retorna o ConjuntoSisreg registrado para a chave dada."""
    if chave not in CONJUNTOS:
        raise KeyError(f"Conjunto '{chave}' desconhecido. Validos: {list(CONJUNTOS)}")
    return CONJUNTOS[chave]
