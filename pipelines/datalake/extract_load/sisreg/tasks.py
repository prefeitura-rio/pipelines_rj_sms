# -*- coding: utf-8 -*-
"""
Tasks genericas do fluxo unificado SISREG.

Cada task cobre uma etapa do DAG:
  resolver_credenciais -> planejar_trabalho -> extrair_item ->
  consolidar -> normalizar_e_subir -> registrar_log_execucao

As tasks nao conhecem os detalhes de cada conjunto: delegam ao registry
(ConjuntoSisreg.planejar_trabalho e .extrair_item) e a common/ para a
logica reusavel. O DAG em flows.py orquestra a ordem.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from prefect import task
from prefect.triggers import all_finished
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.errors import (
    ErroBloqueio,
    ErroEstrutura,
    ErroSisreg,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.registry import obter_conjunto
from pipelines.datalake.extract_load.sisreg.resultado import (  # noqa: F401
    Consolidado,
    ResultadoConjunto,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.credential_injector import authenticated_task as authed_task
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

_FUSO_SP = ZoneInfo("America/Sao_Paulo")


# ---------------------------------------------------------------------------
# Task 0: data de extracao (computada em runtime para particionamento correto)
# ---------------------------------------------------------------------------


@task
def obter_data_extracao() -> str:
    """Retorna a data atual no fuso SP no formato YYYY-MM-DD.

    Computada dentro de uma task para garantir que o valor reflita o momento
    real da extracao, independentemente do horario de disparo do schedule.
    Usada como coluna de particao em todas as tabelas.
    """
    return datetime.now(_FUSO_SP).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Task 1: resolver credenciais
# ---------------------------------------------------------------------------


@task
def resolver_credenciais(conjunto: str, environment: str) -> dict:
    """Busca usuario e senha do perfil de credencial do conjunto no Infisical.

    Retorna dict com 'usuario' e 'senha'. O perfil (padrao/regulador) e
    lido do registry para que mudancas de perfil nao exijam alterar o DAG.
    """
    conj = obter_conjunto(conjunto)
    perfil = C.PERFIS_CREDENCIAL[conj.perfil_credencial]

    usuario = get_secret_key.run(
        secret_path=perfil["caminho"],
        secret_name=perfil["chave_usuario"],
        environment=environment,
    )
    senha = get_secret_key.run(
        secret_path=perfil["caminho"],
        secret_name=perfil["chave_senha"],
        environment=environment,
    )
    log(f"[{conjunto}] Credenciais resolvidas para perfil '{conj.perfil_credencial}'")
    return {"usuario": usuario, "senha": senha}


# ---------------------------------------------------------------------------
# Task 2: planejar trabalho
# ---------------------------------------------------------------------------


@task
def planejar_trabalho(conjunto: str, credenciais: dict, params: dict) -> dict:
    """Determina o item de trabalho para o extrator.

    Delega ao ConjuntoSisreg.planejar_trabalho do registry. O resultado e
    um dict passado diretamente para extrair_item.

    Levanta ErroVazioSuspeito se o dict estiver vazio para conjuntos que
    nunca deveriam ter item vazio (afastamentos, escalas, solicitacoes).
    """
    conj = obter_conjunto(conjunto)
    item = conj.planejar_trabalho(credenciais=credenciais, params=params)

    _conjuntos_nunca_vazios = {"escalas", "afastamentos", "solicitacoes"}
    if not item and conjunto in _conjuntos_nunca_vazios:
        raise ErroVazioSuspeito(
            f"Item de trabalho vazio para '{conjunto}' - suspeito de falha upstream",
            conjunto=conjunto,
            etapa="planejamento",
            detalhe="dict vazio retornado por planejar_trabalho",
        )

    log(f"[{conjunto}] Item de trabalho planejado: id={item.get('id', '?')}")
    return item


# ---------------------------------------------------------------------------
# Task 3: extrair item (mapeada)
# ---------------------------------------------------------------------------


@task(max_retries=2, retry_delay=C.DELAY_MINIMO_S)
def extrair_item(
    conjunto: str,
    item: dict,
    credenciais: dict,
    params: dict,
) -> Optional[ResultadoConjunto]:
    """Extrai um unico item e retorna um ResultadoConjunto.

    Cada chamada e independente: falha de um item nao afeta os demais
    (o trigger all_finished em consolidar absorve os Nones).

    Levanta ErroBloqueio imediatamente (nao retriaria - seria contraproducente).
    Outros ErroSisreg sao logados e retornam None apos esgotar as retries do Prefect.

    Adiciona usuario/senha ao params para que extratores de longa duracao
    (afastamentos, solicitacoes, fila_vagas) possam reautenticar inline em
    caso de expiracao de sessao sem precisar de uma assinatura extra.

    Retorna:
        ResultadoConjunto com os dados e metricas de sub-itens, ou None em falha.
    """
    from pipelines.datalake.extract_load.sisreg.common.auth import (
        abrir_sessao_autenticada,
    )

    conj = obter_conjunto(conjunto)
    item_id = str(item.get("id", item.get("cpf_idx", item.get("data", "item"))))
    log(f"[{conjunto}] Extraindo item: {item_id}")

    # Injeta credenciais nos params para re-auth inline nos extratores longos.
    # Nunca logado; o dict e in-memory e nao persiste alem desta task.
    params_com_cred = {**params, "usuario": credenciais["usuario"], "senha": credenciais["senha"]}

    try:
        sessao = abrir_sessao_autenticada(
            usuario=credenciais["usuario"],
            senha=credenciais["senha"],
            conjunto=conjunto,
        )
        resultado = conj.extrair_item(sessao=sessao, item=item, params=params_com_cred)

        # Normaliza para o contrato ResultadoConjunto.
        # Extratores coarsened (afastamentos, solicitacoes, fila_vagas) retornam
        # ResultadoConjunto diretamente com metricas reais de sub-itens.
        # Extratores simples (escalas, preparos) retornam dict; envolve como
        # ResultadoConjunto(total=1, ok=1) para uniformidade.
        if isinstance(resultado, ResultadoConjunto):
            log(
                f"[{conjunto}/{item_id}] ok - tabelas: {list(resultado.tabelas.keys())}"
                f" ({resultado.ok}/{resultado.total} sub-itens)"
            )
            return resultado
        # resultado e um dict padrao
        log(f"[{conjunto}/{item_id}] Extracao ok - tabelas: {list(resultado.keys())}")
        return ResultadoConjunto(tabelas=resultado, total=1, ok=1)

    except ErroBloqueio as exc:
        # Bloqueio de IP: nao retry, levantar para circuit-break no fluxo.
        log(f"[{conjunto}/{item_id}] BLOQUEIO: {exc}", level="error")
        raise

    except ErroSisreg as exc:
        log(f"[{conjunto}/{item_id}] Erro: {exc}", level="warning")
        return None

    except Exception as exc:  # noqa: BLE001
        log(f"[{conjunto}/{item_id}] Erro inesperado: {type(exc).__name__}: {exc}", level="error")
        return None


# ---------------------------------------------------------------------------
# Task 4: consolidar (trigger all_finished)
# ---------------------------------------------------------------------------


@task(trigger=all_finished)
def consolidar(
    conjunto: str,
    fragmento: Optional[ResultadoConjunto],
    data_extracao: str,
) -> Consolidado:
    """Valida o ResultadoConjunto, adiciona particao e aplica gate de completude.

    O trigger all_finished garante execucao mesmo quando a task upstream falhou.
    None em fragmento indica falha total da task extrair_item; ResultadoConjunto
    com ids_falhos indica falhas parciais de sub-itens (ex.: alguns CPFs).

    Gate de completude (nivel task): fragmento None → upload cancelado.
    Gate de completude (nivel sub-item): qualquer ids_falhos → upload cancelado.
    Dados medicos nao devem ser parcialmente sobrescritos; um dia ruim se corrige
    na proxima execucao bem-sucedida.

    Retorna:
        Consolidado com tabelas e metricas. tabelas=None quando o gate falhou.
    """
    from pipelines.utils.monitor import send_message

    _metricas_vazias: Dict[str, Any] = {
        "items_total": 0,
        "items_ok": 0,
        "linhas_por_tabela": {},
        "ids_falhos": [],
        "status": "FALHA",
    }

    if fragmento is None:
        log(f"[{conjunto}] consolidar: fragmento e None (task upstream falhou)", level="error")
        return Consolidado(tabelas=None, metricas=_metricas_vazias)

    log(f"[{conjunto}] Consolidando: {fragmento.ok}/{fragmento.total} sub-itens ok")

    # Gate de sub-itens: qualquer falha parcial dentro de um extrator coarsened
    if fragmento.ids_falhos:
        msg = (
            f"[{conjunto}] Gate de completude (sub-itens): {len(fragmento.ids_falhos)} falha(s)."
            " Upload cancelado."
        )
        log(msg, level="error")
        try:
            send_message(
                title=f"SISREG - {conjunto}: falha parcial",
                message=msg,
                monitor_slug="warning",
            )
        except Exception:  # noqa: BLE001
            pass
        return Consolidado(
            tabelas=None,
            metricas={
                "items_total": fragmento.total,
                "items_ok": fragmento.ok,
                "linhas_por_tabela": {},
                "ids_falhos": fragmento.ids_falhos,
                "status": "FALHA_PARCIAL",
            },
        )

    # Adicionar coluna de particao e filtrar tabelas vazias
    tabelas_consolidadas: Dict[str, pd.DataFrame] = {}
    for tabela, df in fragmento.tabelas.items():
        if df.empty:
            log(f"[{conjunto}/{tabela}] Tabela vazia - SKIP")
            continue
        df_copy = df.copy()
        df_copy[C.COLUNA_PARTICAO] = data_extracao
        tabelas_consolidadas[tabela] = df_copy
        log(f"[{conjunto}/{tabela}] {len(df_copy)} linhas")

    # Validar schema: colunas obrigatorias ausentes falham em voz alta.
    conj = obter_conjunto(conjunto)
    for tabela, df in tabelas_consolidadas.items():
        esperadas = conj.colunas_esperadas.get(tabela, frozenset())
        if esperadas:
            presentes = set(df.columns)
            ausentes = esperadas - presentes
            if ausentes:
                raise ErroEstrutura(
                    f"Schema drift em '{tabela}': colunas ausentes: {sorted(ausentes)}",
                    conjunto=conjunto,
                    etapa="consolidacao",
                    item=tabela,
                    detalhe=f"esperadas={sorted(esperadas)}, ausentes={sorted(ausentes)}",
                )

    linhas = {t: len(df) for t, df in tabelas_consolidadas.items()}
    if not tabelas_consolidadas:
        return Consolidado(
            tabelas=None, metricas={**_metricas_vazias, "items_total": fragmento.total}
        )

    return Consolidado(
        tabelas=tabelas_consolidadas,
        metricas={
            "items_total": fragmento.total,
            "items_ok": fragmento.ok,
            "linhas_por_tabela": linhas,
            "ids_falhos": [],
            "status": "OK",
        },
    )


# ---------------------------------------------------------------------------
# Task 5: normalizar e subir
# ---------------------------------------------------------------------------


@authed_task()
def normalizar_e_subir(
    environment: str,
    conjunto: str,
    dataset_id: str,
    consolidado: Optional[Consolidado],
) -> bool:
    """Normaliza colunas e faz upload de cada tabela no datalake.

    Usa dump_mode="overwrite" para garantir idempotencia: cada execucao
    bem-sucedida substitui a tabela inteira. So executa se consolidado.tabelas
    nao for None (gate de completude garantido em consolidar).

    Retorna True se o upload foi executado, False se foi pulado.
    """
    if consolidado is None or consolidado.tabelas is None:
        log(f"[{conjunto}] normalizar_e_subir: sem dados (gate falhou ou erro upstream) - SKIP")
        return False

    # Modo de escrita por conjunto: "overwrite" (padrao) ou "append" (paginado).
    modo_escrita = obter_conjunto(conjunto).modo_escrita

    for nome_tabela, df in consolidado.tabelas.items():
        log(
            f"[{conjunto}] Normalizando e subindo tabela '{nome_tabela}' "
            f"({len(df)} linhas, modo={modo_escrita})"
        )
        df_bq = handle_columns_to_bq.run(df=df)
        upload_df_to_datalake.run(
            df=df_bq,
            dataset_id=dataset_id,
            table_id=nome_tabela,
            dump_mode=modo_escrita,
            source_format="parquet",
            partition_column=C.COLUNA_PARTICAO,
        )
        log(f"[{conjunto}] Tabela '{nome_tabela}' enviada com sucesso")

    return True


# ---------------------------------------------------------------------------
# Task 6: registrar log de execucao
# ---------------------------------------------------------------------------


@task(trigger=all_finished)
def registrar_log_execucao(
    conjunto: str,
    consolidado: Optional[Consolidado],
    subiu: bool,
    dataset_id: str,
) -> None:
    """Persiste o registro de saude da execucao no datalake (tabela de log).

    Registrado com trigger all_finished para capturar tambem as execucoes
    que falharam. O monitor de frescor le esta tabela para alertar quando
    uma execucao nao ocorreu dentro do SLA.

    Deriva o status a partir do Consolidado e do bool subiu, garantindo que
    o log reflita a realidade: status="OK" somente quando os dados foram
    efetivamente enviados ao datalake.

    Args:
        conjunto: Chave do conjunto de dados.
        consolidado: Resultado de consolidar (contem metricas reais).
        subiu: True se normalizar_e_subir executou o upload com sucesso.
        dataset_id: Dataset de destino do log.
    """
    agora = datetime.now(_FUSO_SP)
    metricas = consolidado.metricas if consolidado else {}

    # Status derivado do resultado real da execucao:
    # - "OK" somente se os dados foram carregados
    # - "FALHA_PARCIAL" se consolidar detectou falhas parciais
    # - "FALHA" em erro total ou upstream crash
    if subiu:
        status = "OK"
    else:
        status = metricas.get("status", "FALHA")

    items_total = metricas.get("items_total", 0)
    items_ok = metricas.get("items_ok", 0)
    linhas = str(metricas.get("linhas_por_tabela", {}))

    registro: Dict[str, Any] = {
        "conjunto": [conjunto],
        "as_of": [agora.strftime("%Y-%m-%dT%H:%M:%S")],
        "items_total": [items_total],
        "items_ok": [items_ok],
        "linhas_por_tabela": [linhas],
        "status": [status],
        C.COLUNA_PARTICAO: [agora.strftime("%Y-%m-%d")],
    }
    df_log = pd.DataFrame(registro)

    log(f"[{conjunto}] Log de execucao: status={status}, items={items_ok}/{items_total}")

    try:
        upload_df_to_datalake.run(
            df=df_log,
            dataset_id=dataset_id,
            table_id=C.TABELA_LOG_EXECUCAO,
            dump_mode="append",
            source_format="parquet",
            partition_column=C.COLUNA_PARTICAO,
        )
    except Exception as exc:  # noqa: BLE001
        # Log e best-effort: nao pode derrubar o fluxo por falha de auditoria.
        log(f"[{conjunto}] Falha ao registrar log (nao critico): {exc}", level="warning")
