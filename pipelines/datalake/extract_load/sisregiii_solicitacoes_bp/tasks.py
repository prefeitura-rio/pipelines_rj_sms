# -*- coding: utf-8 -*-

import os
import time
import hashlib
import calendar
import pytz
from datetime import datetime, timedelta
import urllib3
import requests
from bs4 import BeautifulSoup
import pandas as pd

from prefect import task
from prefect.engine.signals import FAIL
from prefect.triggers import all_finished
from pipelines.utils.tasks import upload_df_to_datalake

from prefeitura_rio.pipelines_utils.logging import log
from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.constants import (
    BASE_URL,
    GERENCIADOR_URL,
    HEADERS_DISFARCE,
    COLUNAS_SISREG_MAPPING,
    RUN_CONFIGS,
)


# Funções de tratamento, html, login e verificação
def sha256_upper(txt: str) -> str:
    return hashlib.sha256(txt.upper().encode("utf-8")).hexdigest()


def extrair_campos_ocultos(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    campos_ocultos = {}
    for campo in soup.find_all("input", type="hidden"):
        nome_do_campo = campo.get("name")
        if nome_do_campo:
            campos_ocultos[nome_do_campo] = campo.get("value", "")
    return campos_ocultos


# Extrai o nome da imagem atribuída a cor a partir do link (src)
def extrair_cor_do_src(src: str) -> str:
    return os.path.splitext(os.path.basename(src))[0]


def limpar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(r"[^a-zA-Z0-9]+", "_", regex=True)
        .str.strip("_")
        .str.lower()
    )

    vistos = set()
    novas_colunas = []
    for col in df.columns:
        nova_col = col
        contador = 1
        while nova_col in vistos:
            nova_col = f"{col}_{contador}"
            contador += 1
        vistos.add(nova_col)
        novas_colunas.append(nova_col)

    df.columns = novas_colunas
    return df


def realizar_login(sessao: requests.Session, usuario: str, senha: str, timeout_login: int) -> None:
    r_get = sessao.get(BASE_URL, timeout=timeout_login)
    r_get.raise_for_status()

    soup = BeautifulSoup(r_get.text, "html.parser")
    form_login = soup.find("form")

    if form_login and form_login.has_attr("action"):
        action = form_login["action"]
        url_post_real = (
            action
            if action.startswith("http")
            else f"{BASE_URL}{action if action.startswith('/') else '/' + action}"
        )
    else:
        raise FAIL("Não encontrei a tag <form> na página inicial.")

    campos_ocultos = extrair_campos_ocultos(r_get.text)
    payload = {
        **campos_ocultos,
        "usuario": usuario,
        "senha": "",
        "senha_256": sha256_upper(senha),
        "etapa": "ACESSO",
        "entrar": "Entrar",
    }

    r_post = sessao.post(url_post_real, data=payload, allow_redirects=True, timeout=timeout_login)
    texto_pagina = r_post.text.lower()

    texto_sucesso = ("sair", "menu", "bem-vindo")
    if any(palavra in texto_pagina for palavra in texto_sucesso):
        log("Login realizado com sucesso.")
        return True
    else:
        raise FAIL("Falha no login do Sisreg.")


def criar_sessao_autenticada(usuario: str, senha: str, timeout_login: int) -> requests.Session:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    sessao = requests.Session()
    sessao.headers.update(HEADERS_DISFARCE)
    sessao.verify = False
    realizar_login(sessao, usuario, senha, timeout_login)
    return sessao


def _verificar_resposta_html(texto_html: str) -> str:
    texto_lower = texto_html.lower()
    if any(msg in texto_lower for msg in ("sua sessão foi finalizada", "efetue o logon novamente")):
        return "SESSAO_ZUMBI"
    if any(
        msg in texto_lower for msg in ("erro não esperado", "erro nao esperado", "erro inesperado")
    ):
        return "ERRO_CRITICO"
    if "nenhum registro encontrado" in texto_lower:
        return "VAZIO"
    return "OK"


def _extrair_tabela_do_html(texto_html: str, status_req: str) -> pd.DataFrame:
    soup = BeautifulSoup(texto_html, "html.parser")
    tables = soup.find_all("table", class_="table_listagem")

    tabela_alvo = None
    for tb in tables:
        texto_tb = tb.text.lower()
        colunas_obrigatorias = ("cód. solicitação", "data da solicitação", "risco")
        if all(coluna in texto_tb for coluna in colunas_obrigatorias):
            tabela_alvo = tb
            break

    if tabela_alvo:
        rows = tabela_alvo.find_all("tr")
        headers = [col.text.strip() for col in rows[1].find_all(["td", "th"])]

        try:
            risco_index = headers.index("Risco")
        except ValueError:
            risco_index = None

        data = []
        for row in rows[2:]:
            cols = row.find_all("td")
            if not cols:
                continue

            row_data = [col.text.strip() for col in cols]

            if risco_index is not None and risco_index < len(cols):
                img_tag = cols[risco_index].find("img")
                row_data[risco_index] = extrair_cor_do_src(img_tag["src"]) if img_tag else None

            data.append(row_data)

        if data:
            df_resultado = pd.DataFrame(data, columns=headers)
            df_resultado["status"] = status_req
            return limpar_nomes_colunas(df_resultado)
        else:
            return pd.DataFrame()
    else:
        raise ValueError(f"A tabela não foi renderizada no HTML para o status {status_req}.")


# TASKS
@task(max_retries=30, retry_delay=timedelta(seconds=20))
def extrair_item_sisreg(
    usuario: str,
    senha: str,
    ficha: dict,
    timeout_login: int,
    timeout_consulta: int,
    tempo_espera: float,
) -> pd.DataFrame:

    data_req = ficha["data"]
    tipo_per = ficha["config"]["tipo_pedido"]
    cod_sit = ficha["config"]["codigo_situacao"]
    nome_stat = ficha["config"]["status_coluna"]

    log(f"Iniciando requisição isolada: {data_req} [{nome_stat}]")

    sessao = criar_sessao_autenticada(usuario, senha, timeout_login)

    url_consulta = (
        f"{GERENCIADOR_URL}?etapa=LISTAR_SOLICITACOES"
        f"&co_solicitacao=&cns_paciente=&no_usuario=&cnes_solicitante=&cnes_executante=&co_proc_unificado="
        f"&co_pa_interno=&ds_procedimento=&tipo_periodo={tipo_per}"
        f"&dt_inicial={data_req}&dt_final={data_req}"
        f"&cmb_situacao={cod_sit}&qtd_itens_pag=0&co_seq_solicitacao=&ordenacao=2&pagina=0"
    )

    try:
        resposta = sessao.get(url_consulta, timeout=timeout_consulta)
        time.sleep(tempo_espera)
        sessao.close()
    except requests.exceptions.RequestException as e:
        sessao.close()
        raise ValueError(
            f"Falha de conexão com o Sisreg ({type(e).__name__}). O Prefect ativará a retentativa automática."
        ) from None

    status_pagina = _verificar_resposta_html(resposta.text)

    if status_pagina == "SESSAO_ZUMBI":
        raise ValueError("Sessão Zumbi detectada. O Prefect ativará a retentativa automática.")
    if status_pagina == "ERRO_CRITICO":
        raise ValueError("Erro interno do Sisreg. O Prefect ativará a retentativa automática.")
    if status_pagina == "VAZIO":
        return pd.DataFrame()

    return _extrair_tabela_do_html(resposta.text, nome_stat)


@task
def gerar_roteiro_extracao(
    data_especifica: str | None = None,
    data_inicial: str | None = None,
    data_final: str | None = None,
    status_especifico: str | list | None = None,
) -> list:

    fuso_br = pytz.timezone("America/Sao_Paulo")
    datas = []

    if data_inicial and data_final:
        try:
            dt_inicio = datetime.strptime(data_inicial, "%d/%m/%Y")
            dt_fim = datetime.strptime(data_final, "%d/%m/%Y")
            if dt_inicio > dt_fim:
                raise ValueError("A data_inicial não pode ser maior que a data_final.")

            delta = dt_fim - dt_inicio
            for i in range(delta.days + 1):
                datas.append((dt_inicio + timedelta(days=i)).strftime("%d/%m/%Y"))
            log(f"Modo Período: Extraindo {len(datas)} dias.")
        except ValueError as e:
            raise FAIL(f"Erro ao processar período: {e}")

    elif data_especifica:
        if data_especifica.lower() == "ontem":
            data_especifica = (datetime.now(fuso_br) - timedelta(days=1)).strftime("%d/%m/%Y")
        datas = [data_especifica]

    else:
        hoje = datetime.now(fuso_br)
        mes_anterior = hoje.replace(day=1) - timedelta(days=1)
        ano, mes = mes_anterior.year, mes_anterior.month
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        datas = [(f"{dia:02d}/{mes:02d}/{ano}") for dia in range(1, ultimo_dia + 1)]
        log(f"Modo Mensal: Foram geradas {len(datas)} datas.")

    todas_configs = list(RUN_CONFIGS.values())
    configs_finais = todas_configs

    if status_especifico:
        if isinstance(status_especifico, str):
            lista_status = [s.strip().upper() for s in status_especifico.split(",")]
        elif isinstance(status_especifico, list):
            lista_status = [str(s).strip().upper() for s in status_especifico]
        else:
            lista_status = []

        configs_finais = [c for c in todas_configs if c["status_coluna"] in lista_status]
        if not configs_finais:
            configs_finais = todas_configs

    fichas_de_extracao = []
    for d in datas:
        for c in configs_finais:
            fichas_de_extracao.append({"data": d, "config": c})

    return fichas_de_extracao


# trigger=all_finished garante que o pipeline guarde os sucessos mesmo se houver falhas irrecuperáveis
@task(trigger=all_finished)
def consolidar_e_salvar(lista_de_dfs: list, dataset_id: str, table_id: str) -> None:

    dfs_validos = [df for df in lista_de_dfs if isinstance(df, pd.DataFrame) and not df.empty]
    falhas = len(lista_de_dfs) - len(dfs_validos)

    if falhas > 0:
        log(f"⚠️ Atenção: {falhas} requisições falharam completamente após 30 retentativas.")

    if dfs_validos:
        dfs_traduzidos = []
        for df in dfs_validos:
            df.rename(columns=COLUNAS_SISREG_MAPPING, inplace=True)
            df = df.loc[:, ~df.columns.str.contains("^unnamed|erro", case=False)]
            dfs_traduzidos.append(df)

        df_final = pd.concat(dfs_traduzidos, ignore_index=True)
        df_final["data_particao"] = datetime.now().strftime("%Y-%m-%d")

        log(
            f"Iniciando envio para o DataLake: {dataset_id}.{table_id} ({len(df_final)} registros unificados)"
        )

        upload_df_to_datalake.run(
            df=df_final,
            dataset_id=dataset_id,
            table_id=table_id,
            partition_column="data_particao",
            source_format="parquet",
        )
        log("✅ Dados enviados para o DataLake com sucesso!")
    else:
        log("Nenhuma tabela válida foi extraída. O DataLake não será atualizado.")
