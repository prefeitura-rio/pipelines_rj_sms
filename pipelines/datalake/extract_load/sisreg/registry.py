# -*- coding: utf-8 -*-
"""
Registry de conjuntos de dados do SISREG.

Define o dataclass ConjuntoSisreg e o dicionario CONJUNTOS com os 5 datasets
do fluxo unificado. Cada conjunto declara apenas o que o diferencia dos demais:
perfil de credencial, tabelas de destino, colunas esperadas e as funcoes
planejar_trabalho/extrair_item especificas.

A logica generica (autenticacao, politeness, consolidacao, upload, log) fica
em tasks.py e common/. Nenhum conjunto precisa reimplementar isso.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, List, Tuple

from pipelines.datalake.extract_load.sisreg import constants as C

# ---------------------------------------------------------------------------
# Defaults que falham em voz alta se o extrator nao foi registrado.
# Substitui lambdas silenciosas que mascaravam ImportErrors em producao.
# ---------------------------------------------------------------------------


def _planejar_nao_registrado(credenciais: dict, params: dict) -> dict:
    """Levantado quando o extrator ainda nao foi registrado no CONJUNTOS."""
    raise NotImplementedError(
        "planejar_trabalho nao registrado. Extrator nao importado ou falha em _importar_extratores."
    )


def _extrair_nao_registrado(sessao: Any, item: dict, params: dict) -> dict:
    """Levantado quando o extrator ainda nao foi registrado no CONJUNTOS."""
    raise NotImplementedError(
        "extrair_item nao registrado. Extrator nao importado ou falha em _importar_extratores."
    )


@dataclass(frozen=True)
class ConjuntoSisreg:
    """Configuracao imutavel de um conjunto de dados do SISREG.

    Todos os campos sao obrigatorios exceto 'colunas_esperadas', que pode ser
    vazio quando o schema e validado pelo extrator (ex.: fila_vagas com duas
    tabelas de estrutura distinta).
    """

    # Identificador unico do conjunto (chave no dicionario CONJUNTOS).
    chave: str

    # Perfil de credencial a usar: "padrao" (/sisreg) ou "regulador" (/sisreg_regulacao).
    # Deve ser uma chave valida em constants.PERFIS_CREDENCIAL.
    perfil_credencial: str

    # Nomes das tabelas de destino no datalake (1 ou 2 por conjunto).
    tabelas: Tuple[str, ...]

    # Colunas esperadas por tabela: {nome_tabela: frozenset(colunas)}.
    # Usadas em consolidar() para validar schema e detectar deriva silenciosa.
    colunas_esperadas: Dict[str, FrozenSet[str]] = field(default_factory=dict)

    # Funcao que determina o item de trabalho (plano) do conjunto.
    # Cada conjunto produz UM unico item por execucao (single-flight, uma
    # sessao por run); o loop sobre sub-itens fica dentro do extrator.
    # Assinatura: (credenciais: dict, params: dict) -> dict
    planejar_trabalho: Callable = field(default=_planejar_nao_registrado)

    # Funcao que extrai o item e retorna os dados por tabela.
    # Assinatura: (sessao, item: dict, params: dict) -> ResultadoConjunto | dict
    extrair_item: Callable = field(default=_extrair_nao_registrado)

    # Modo de escrita no datalake: "overwrite" (padrao - substitui a tabela a
    # cada execucao 100% completa) ou "append" (acrescenta as linhas do run).
    # preparos usa "append" porque e paginado por shard (cada run cobre so um
    # subconjunto de unidades); a deduplicacao "ultima por unidade" e resolvida
    # na camada de modelagem. Nunca use overwrite com um extrator paginado - isso
    # apagaria os shards das execucoes anteriores.
    modo_escrita: str = "overwrite"


def _importar_extratores() -> None:
    """Importa os modulos de extratores para que registrem suas funcoes.

    Importacao tardia para evitar ciclos: os extratores importam de registry,
    entao registry nao pode importa-los no nivel de modulo.

    Nao tolera ImportError: um erro real de importacao (dependencia faltando,
    typo, etc.) deve propagar e causar falha visivel, nunca degradar para os
    defaults NotImplementedError que mascariam a falha como no-op silencioso.
    """
    from pipelines.datalake.extract_load.sisreg.extractors import (  # noqa: F401
        afastamentos,
        escalas,
        fila_vagas,
        preparos,
        solicitacoes,
    )


# ---------------------------------------------------------------------------
# Definicao dos 5 conjuntos
# ---------------------------------------------------------------------------

CONJUNTO_ESCALAS = ConjuntoSisreg(
    chave="escalas",
    perfil_credencial="padrao",
    tabelas=(C.TABELA_ESCALAS,),
    colunas_esperadas={
        # Colunas confiadas conforme instrucao: o legado e a fonte de verdade.
        # Validacao de schema sera refinada apos primeira execucao em staging.
        C.TABELA_ESCALAS: frozenset(),
    },
)

CONJUNTO_AFASTAMENTOS = ConjuntoSisreg(
    chave="afastamentos",
    perfil_credencial="regulador",
    tabelas=(C.TABELA_AFASTAMENTOS, C.TABELA_AFASTAMENTO_HISTORICO),
    colunas_esperadas={
        C.TABELA_AFASTAMENTOS: frozenset(
            ["cpf", "data_inicio", "data_fim", "hora_inicio", "hora_fim", C.COLUNA_PARTICAO]
        ),
        C.TABELA_AFASTAMENTO_HISTORICO: frozenset(["cpf", "data", C.COLUNA_PARTICAO]),
    },
)

CONJUNTO_PREPAROS = ConjuntoSisreg(
    chave="preparos",
    perfil_credencial="padrao",
    tabelas=(C.TABELA_PREPAROS,),
    colunas_esperadas={
        C.TABELA_PREPAROS: frozenset(
            ["cnes", "nome_unidade", "cod_procedimento", "nome_procedimento", "descricao_preparo"]
        ),
    },
    # Paginado por shard de unidades -> append (nao overwrite). Ver preparos.py.
    modo_escrita="append",
)

CONJUNTO_SOLICITACOES = ConjuntoSisreg(
    chave="solicitacoes",
    perfil_credencial="padrao",
    tabelas=(C.TABELA_SOLICITACOES,),
    colunas_esperadas={
        C.TABELA_SOLICITACOES: frozenset(),
    },
)

CONJUNTO_FILA_VAGAS = ConjuntoSisreg(
    chave="fila_vagas",
    perfil_credencial="regulador",
    tabelas=(C.TABELA_FILA_E_VAGAS, C.TABELA_VAGAS_DETALHADAS),
    colunas_esperadas={
        C.TABELA_FILA_E_VAGAS: frozenset(
            ["cod_interno_proced", "cnes_unidade", "qtd_pend", "qtd_vagas", C.COLUNA_PARTICAO]
        ),
        C.TABELA_VAGAS_DETALHADAS: frozenset(
            ["cod_interno_proced", "cnes_unidade", "data_vaga", C.COLUNA_PARTICAO]
        ),
    },
)

# Dicionario principal: chave -> ConjuntoSisreg.
# Os extratores substituem as funcoes planejar_trabalho/extrair_item de cada
# conjunto via CONJUNTOS[chave] = replace(...) apos sua importacao.
CONJUNTOS: Dict[str, ConjuntoSisreg] = {
    c.chave: c
    for c in [
        CONJUNTO_ESCALAS,
        CONJUNTO_AFASTAMENTOS,
        CONJUNTO_PREPAROS,
        CONJUNTO_SOLICITACOES,
        CONJUNTO_FILA_VAGAS,
    ]
}


def registrar_extrator(
    chave: str,
    planejar_trabalho: Callable,
    extrair_item: Callable,
) -> None:
    """Registra as funcoes de extracao de um conjunto no CONJUNTOS global.

    Chamado pelo modulo do extrator (ex.: extractors/escalas.py) durante
    sua importacao. Usa dataclasses.replace para preservar a imutabilidade.
    """
    import dataclasses

    if chave not in CONJUNTOS:
        raise KeyError(
            f"Conjunto '{chave}' nao existe no registry. Chaves validas: {list(CONJUNTOS)}"
        )
    CONJUNTOS[chave] = dataclasses.replace(
        CONJUNTOS[chave],
        planejar_trabalho=planejar_trabalho,
        extrair_item=extrair_item,
    )


def obter_conjunto(chave: str) -> ConjuntoSisreg:
    """Retorna o ConjuntoSisreg registrado para a chave dada.

    Garante que os extratores foram importados antes de acessar as funcoes.
    """
    _importar_extratores()
    if chave not in CONJUNTOS:
        chaves_validas = list(CONJUNTOS.keys())
        raise KeyError(f"Conjunto '{chave}' desconhecido. Validos: {chaves_validas}")
    return CONJUNTOS[chave]


def listar_conjuntos() -> List[str]:
    """Retorna a lista de chaves registradas no CONJUNTOS."""
    return list(CONJUNTOS.keys())
