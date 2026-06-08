# -*- coding: utf-8 -*-
"""
Tasks genericas do fluxo unificado SISREG.

Cada task cobre uma etapa do DAG map-reduce:
  resolver_credenciais -> planejar_trabalho -> extrair_item.map ->
  consolidar -> normalizar_e_subir -> registrar_log_execucao

As tasks nao conhecem os detalhes de cada conjunto: delegam ao registry
(ConjuntoSisreg.planejar_trabalho e .extrair_item) e a common/ para a
logica reusavel. O DAG em flows.py orquestra a ordem e o mapeamento.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
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
def planejar_trabalho(conjunto: str, credenciais: dict, params: dict) -> List[dict]:
    """Determina a lista de itens de trabalho para o .map do extrator.

    Delega ao ConjuntoSisreg.planejar_trabalho do registry. O resultado e
    uma lista de dicts - cada dict e um 'item' passado para extrair_item.

    Levanta ErroVazioSuspeito se a lista estiver vazia para conjuntos que
    nunca deveriam ter lista vazia (afastamentos, escalas).
    """
    conj = obter_conjunto(conjunto)
    items = conj.planejar_trabalho(credenciais=credenciais, params=params)

    _conjuntos_nunca_vazios = {"escalas", "afastamentos", "solicitacoes"}
    if not items and conjunto in _conjuntos_nunca_vazios:
        raise ErroVazioSuspeito(
            f"Lista de trabalho vazia para '{conjunto}' - suspeito de falha upstream",
            conjunto=conjunto,
            etapa="planejamento",
            detalhe="0 itens retornados por planejar_trabalho",
        )

    log(f"[{conjunto}] {len(items)} item(s) planejado(s) para extracao")
    return items


# ---------------------------------------------------------------------------
# Task 3: extrair item (mapeada)
# ---------------------------------------------------------------------------


@task(max_retries=2, retry_delay=C.DELAY_MINIMO_S)
def extrair_item(
    conjunto: str,
    item: dict,
    credenciais: dict,
    params: dict,
) -> Optional[Dict[str, pd.DataFrame]]:
    """Extrai um unico item e retorna fragmentos por tabela.

    Cada chamada e independente: falha de um item nao afeta os demais
    (o trigger all_finished em consolidar absorve os Nones).

    Levanta ErroBloqueio imediatamente (nao retriaria - seria contraproducente).
    Outros ErroSisreg sao logados e retornam None apos esgotar as retries do Prefect.

    Retorna:
        Dict {nome_tabela: DataFrame} com os dados extraidos, ou None em falha.
    """
    from pipelines.datalake.extract_load.sisreg.common.auth import (
        abrir_sessao_autenticada,
    )

    conj = obter_conjunto(conjunto)
    item_id = str(item.get("id", item.get("cpf_idx", item.get("data", "item"))))
    log(f"[{conjunto}] Extraindo item: {item_id}")

    try:
        sessao = abrir_sessao_autenticada(
            usuario=credenciais["usuario"],
            senha=credenciais["senha"],
            conjunto=conjunto,
        )
        fragmentos = conj.extrair_item(sessao=sessao, item=item, params=params)
        log(f"[{conjunto}/{item_id}] Extracao ok - tabelas: {list(fragmentos.keys())}")
        return fragmentos

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
    fragmentos_lista: List[Optional[Dict[str, pd.DataFrame]]],
    data_extracao: str,
) -> Optional[Dict[str, pd.DataFrame]]:
    """Concatena fragmentos por tabela, valida schema e aplica gate de completude.

    O trigger all_finished garante que esta task roda mesmo quando alguns
    itens falharam. Fragmentos None (falhas) sao descartados antes de
    concatenar.

    Gate de completude: overwrite so ocorre quando TODOS os itens tiverem
    retornado dados. Qualquer falha resulta em SKIP do upload e alerta.
    Dados medicos nao devem ser parcialmente sobrescritos.

    Retorna:
        Dict {tabela: DataFrame consolidado} pronto para upload, ou None se
        o gate de completude falhar ou nao houver dados.
    """
    from pipelines.utils.monitor import send_message

    if fragmentos_lista is None:
        log(f"[{conjunto}] consolidar: fragmentos_lista e None (upstream falhou)", level="error")
        return None

    validos = [f for f in fragmentos_lista if f is not None]
    total = len(fragmentos_lista)
    ok = len(validos)
    falhas = total - ok

    log(f"[{conjunto}] Consolidando: {ok}/{total} itens bem-sucedidos ({falhas} falhas)")

    # Gate de completude: exige 100% de sucesso antes de sobrescrever.
    # Dados medicos parcialmente sobrescritos podem causar decisoes erradas.
    if falhas > 0:
        msg = (
            f"[{conjunto}] Gate de completude FALHOU: {falhas}/{total} itens com erro. "
            "Upload cancelado - tabela anterior permanece intacta."
        )
        log(msg, level="error")
        try:
            send_message(
                title=f"SISREG - {conjunto}: falha parcial",
                message=msg,
                monitor_slug="warning",
            )
        except Exception:  # noqa: BLE001
            pass  # alerta e best-effort
        return None

    if not validos:
        log(f"[{conjunto}] Nenhum fragmento valido apos consolidacao - SKIP", level="warning")
        return None

    # Concatenar por tabela
    tabelas_consolidadas: Dict[str, pd.DataFrame] = {}
    todas_tabelas: set = set()
    for frag in validos:
        todas_tabelas.update(frag.keys())

    for tabela in todas_tabelas:
        dfs = [f[tabela] for f in validos if tabela in f and not f[tabela].empty]
        if not dfs:
            log(f"[{conjunto}/{tabela}] Tabela vazia apos consolidacao - SKIP")
            continue
        df_concat = pd.concat(dfs, ignore_index=True)
        df_concat[C.COLUNA_PARTICAO] = data_extracao
        tabelas_consolidadas[tabela] = df_concat
        log(f"[{conjunto}/{tabela}] {len(df_concat)} linhas consolidadas")

    # Validar schema: colunas obrigatorias ausentes falham em voz alta.
    # Tabelas com colunas_esperadas vazio (ex.: escalas, solicitacoes) pulam
    # a checagem - serao refinadas apos primeira execucao em staging.
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

    return tabelas_consolidadas if tabelas_consolidadas else None


# ---------------------------------------------------------------------------
# Task 5: normalizar e subir
# ---------------------------------------------------------------------------


@authed_task()
def normalizar_e_subir(
    environment: str,
    conjunto: str,
    dataset_id: str,
    tabelas_consolidadas: Optional[Dict[str, pd.DataFrame]],
) -> bool:
    """Normaliza colunas e faz upload de cada tabela no datalake.

    Usa dump_mode="overwrite" para garantir idempotencia: cada execucao
    bem-sucedida substitui a tabela inteira. So executa se tabelas_consolidadas
    nao for None (gate de completude ja garantido em consolidar).

    Retorna True se o upload foi executado, False se foi pulado.
    """
    if tabelas_consolidadas is None:
        log(f"[{conjunto}] normalizar_e_subir: sem dados (gate anterior falhou) - SKIP")
        return False

    for nome_tabela, df in tabelas_consolidadas.items():
        log(f"[{conjunto}] Normalizando e subindo tabela '{nome_tabela}' ({len(df)} linhas)")
        df_bq = handle_columns_to_bq.run(df=df)
        upload_df_to_datalake.run(
            df=df_bq,
            dataset_id=dataset_id,
            table_id=nome_tabela,
            dump_mode="overwrite",
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
    items_total: int,
    items_ok: int,
    linhas_por_tabela: Optional[Dict[str, int]],
    status: str,
    dataset_id: str,
) -> None:
    """Persiste o registro de saude da execucao no datalake (tabela de log).

    Registrado com trigger all_finished para capturar tambem as execucoes
    que falharam. O monitor de frescor (C18) le esta tabela para alertar
    quando uma execucao nao ocorreu dentro do SLA.

    Args:
        conjunto: Chave do conjunto de dados.
        items_total: Total de itens planejados.
        items_ok: Itens extraidos com sucesso.
        linhas_por_tabela: {tabela: nlinhas} ou None se o upload nao ocorreu.
        status: "OK", "FALHA_PARCIAL" ou "FALHA".
        dataset_id: Dataset de destino do log.
    """
    agora = datetime.now(_FUSO_SP)
    registro: Dict[str, Any] = {
        "conjunto": [conjunto],
        "as_of": [agora.strftime("%Y-%m-%dT%H:%M:%S")],
        "items_total": [items_total],
        "items_ok": [items_ok],
        "linhas_por_tabela": [str(linhas_por_tabela or {})],
        "status": [status],
        C.COLUNA_PARTICAO: [agora.strftime("%Y-%m-%d")],
    }
    df_log = pd.DataFrame(registro)

    log(
        f"[{conjunto}] Registrando log de execucao: status={status}, items={items_ok}/{items_total}"
    )

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
