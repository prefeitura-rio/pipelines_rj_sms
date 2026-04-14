# -*- coding: utf-8 -*-

import os
import time
import hashlib
import calendar
import pytz
import logging
from datetime import datetime, timedelta
import urllib3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import prefect
from prefect import task
from prefect.engine.signals import FAIL
from pipelines.utils.tasks import upload_df_to_datalake

from pipelines.datalake.extract_load.sisregiii_solicitacoes_bp.constants import (
    BASE_URL,
    GERENCIADOR_URL,
    HEADERS_DISFARCE,
    TRADUTOR_COLUNAS_SISREG,
    CONFIGS_EXTRACAO_BASE
)


# Funções de tratamento, html, login e verificação
def sha256_upper(txt: str) -> str:
    return hashlib.sha256(txt.upper().encode("utf-8")).hexdigest()

def hidden(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    return {
        i["name"]: i.get("value", "")
        for i in soup.find_all("input", type="hidden")
        if i.get("name")
    }

def extrair_cor_do_src(src: str) -> str:
    return os.path.splitext(os.path.basename(src))[0]

def limpar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        .str.replace(r'[^a-zA-Z0-9]', '_', regex=True) 
        .str.replace(r'_+', '_', regex=True)           
        .str.strip('_')                                
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


def realizar_login(sessao: requests.Session, usuario: str, senha: str, timeout_login: int) -> bool:
    logger = prefect.context.get("logger")
    r_get = sessao.get(BASE_URL, timeout=timeout_login)
    r_get.raise_for_status()
    
    soup = BeautifulSoup(r_get.text, "html.parser")
    form_login = soup.find('form') 
    
    if form_login and form_login.has_attr('action'):
        action = form_login['action']
        url_post_real = action if action.startswith('http') else f"{BASE_URL}{action if action.startswith('/') else '/' + action}"
    else:
        raise FAIL("Não encontrei a tag <form> na página inicial.")

    campos_ocultos = hidden(r_get.text)
    payload = {
        **campos_ocultos,
        "usuario": usuario, "senha": "",
        "senha_256": sha256_upper(senha),
        "etapa": "ACESSO", "entrar": "Entrar"
    }
    
    r_post = sessao.post(url_post_real, data=payload, allow_redirects=True, timeout=timeout_login)
    texto_pagina = r_post.text.lower()
    
    if "sair" in texto_pagina or "menu" in texto_pagina or "bem-vindo" in texto_pagina:
        logger.info("Login realizado com sucesso.")
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
    
    if "sua sessão foi finalizada" in texto_lower or "efetue o logon novamente" in texto_lower:
        return "SESSAO_ZUMBI"
    if "erro não esperado" in texto_lower or "erro nao esperado" in texto_lower or "erro inesperado" in texto_lower:
        raise ValueError("Sisreg crashou com 'Erro não esperado'.")
    if "nenhum registro encontrado" in texto_lower:
        return "VAZIO"
        
    return "OK"

def _extrair_tabela_do_html(texto_html: str, status_req: str) -> pd.DataFrame:
    soup = BeautifulSoup(texto_html, 'html.parser')
    tables = soup.find_all('table', class_='table_listagem')
    
    if len(tables) > 1:
        rows = tables[1].find_all('tr')
        headers = [col.text.strip() for col in rows[1].find_all(['td', 'th'])]
        data = []
        for row in rows[2:]:
            cols = row.find_all('td')
            if not cols: continue
            row_data = [col.text.strip() for col in cols]
            try:
                risco_index = headers.index('Risco')
                img_tag = cols[risco_index].find('img')
                row_data[risco_index] = extrair_cor_do_src(img_tag['src']) if img_tag else None
            except ValueError:
                pass 
            data.append(row_data)

        if data:
            df_resultado = pd.DataFrame(data, columns=headers)
            df_resultado['status'] = status_req 
            df_resultado = limpar_nomes_colunas(df_resultado) 
            return df_resultado
        else:
            return pd.DataFrame()
    else:
        raise ValueError("A tabela não foi renderizada no HTML, mesmo com status OK.")

def _processar_pagina_sisreg(sessao: requests.Session, usuario: str, senha: str, data_req: str, periodo_req: str, status_req: str, cod_situacao_req: str, logger: logging.Logger, timeout_consulta: int, tempo_espera: float, timeout_login: int) -> pd.DataFrame:
    url_consulta = (
        f"{GERENCIADOR_URL}?etapa=LISTAR_SOLICITACOES"
        f"&co_solicitacao=&cns_paciente=&no_usuario=&cnes_solicitante=&cnes_executante=&co_proc_unificado="
        f"&co_pa_interno=&ds_procedimento=&tipo_periodo={periodo_req}"
        f"&dt_inicial={data_req}&dt_final={data_req}"
        f"&cmb_situacao={cod_situacao_req}&qtd_itens_pag=0&co_seq_solicitacao=&ordenacao=2&pagina=0"
    )

    resposta = sessao.get(url_consulta, timeout=timeout_consulta)
    time.sleep(tempo_espera)
    
    status_pagina = _verificar_resposta_html(resposta.text)

    if status_pagina == "SESSAO_ZUMBI":
        logger.warning(f"Sessão zumbi detectada. Relogando para {data_req}...")
        realizar_login(sessao, usuario, senha, timeout_login)
        resposta = sessao.get(url_consulta, timeout=timeout_consulta)
        status_pagina = _verificar_resposta_html(resposta.text)

    if status_pagina == "VAZIO":
        return pd.DataFrame()

    return _extrair_tabela_do_html(resposta.text, status_req)

def _executar_com_retentativa(sessao: requests.Session, usuario: str, senha: str, erro_item: dict, logger: logging.Logger, timeout_consulta: int, tempo_espera: float, timeout_login: int, max_tentativas: int) -> pd.DataFrame | None:
    data_req, nome_req = erro_item["data"], erro_item["nome_status"]
    cod_req, per_req = erro_item["codigo_situacao"], erro_item["tipo_periodo"]
    
    for tentativa in range(1, max_tentativas + 1):
        logger.info(f"🔄 Tentativa {tentativa}/{max_tentativas} -> {data_req} [{per_req}-{nome_req}]")
        try:
            df = _processar_pagina_sisreg(sessao, usuario, senha, data_req, per_req, nome_req, cod_req, logger, timeout_consulta, tempo_espera, timeout_login)
            logger.info(f"✅ Sucesso na reextração de {data_req}!")
            return df
        except Exception as e:
            logger.error(f"Falha na tentativa {tentativa}: {e}")
            time.sleep(10)
            
    return None

# TASKS

@task
def gerar_roteiro_extracao(
    data_especifica: str | None = None, 
    data_inicial: str | None = None, 
    data_final: str | None = None,
    status_especifico: str | None = None
) -> dict:
    
    logger = prefect.context.get("logger")
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
                dia = dt_inicio + timedelta(days=i)
                datas.append(dia.strftime("%d/%m/%Y"))
                
            logger.info(f"Modo Período ativado: Extraindo de {data_inicial} até {data_final} ({len(datas)} dias).")
        except ValueError as e:
            logger.error(f"Erro de formato nas datas. Use DD/MM/AAAA. Detalhe: {e}")
            raise FAIL(f"Erro ao processar período: {e}")

    elif data_especifica:
        if data_especifica.lower() == "ontem":
            dia_anterior = datetime.now(fuso_br) - timedelta(days=1)
            data_especifica = dia_anterior.strftime("%d/%m/%Y")
            logger.info(f"Modo Diário ativado: Calculado 'ontem' como {data_especifica}")

        logger.info(f"A extrair apenas a data {data_especifica}")
        datas = [data_especifica]

    else:
        hoje = datetime.now(fuso_br)
        mes_anterior = hoje.replace(day=1) - timedelta(days=1)
        ano, mes = mes_anterior.year, mes_anterior.month
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        datas = [(f"{dia:02d}/{mes:02d}/{ano}") for dia in range(1, ultimo_dia + 1)]
        logger.info(f"Modo Mensal ativado: Foram geradas {len(datas)} datas para o mês {mes:02d}/{ano}.")

    configs_finais = CONFIGS_EXTRACAO_BASE
    if status_especifico:
        status_upper = status_especifico.upper()
        configs_finais = [c for c in CONFIGS_EXTRACAO_BASE if c["status_coluna"] == status_upper]
        
        if not configs_finais:
            logger.warning(f"Status '{status_upper}' não encontrado. Vai extrair todos os status por segurança.")
            configs_finais = CONFIGS_EXTRACAO_BASE
        else:
            logger.info(f"Modo Filtro ativado: Extraindo APENAS o status '{status_upper}'.")

    return {"datas": datas, "configs": configs_finais}


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def extrair_fase_principal(
    usuario: str, senha: str, roteiro: dict, timeout_login: int, timeout_consulta: int, tempo_espera: float, limite_requisicoes: int
) -> dict:
    
    logger = prefect.context.get("logger")
    datas, configs_extracao = roteiro["datas"], roteiro["configs"]
    dfs_fase1 = []
    datas_com_erro = []
    contador_requisicoes = 0
    
    sessao = criar_sessao_autenticada(usuario, senha, timeout_login)

    for data_do_dia in datas:
        for config in configs_extracao:
            tipo_per, cod_sit, nome_stat = config["tipo_periodo"], config["codigo_situacao"], config["status_coluna"]
            
            if contador_requisicoes > 0 and contador_requisicoes % limite_requisicoes == 0:
                logger.info("A renovar a sessão proativamente...")
                sessao.close()
                sessao = criar_sessao_autenticada(usuario, senha, timeout_login)

            logger.info(f"[FASE 1] Extração: {data_do_dia} | Período: {tipo_per} | Status: {nome_stat}")
            
            try:
                df = _processar_pagina_sisreg(sessao, usuario, senha, data_do_dia, tipo_per, nome_stat, cod_sit, logger, timeout_consulta, tempo_espera, timeout_login)
                if not df.empty:
                    dfs_fase1.append(df)
            except Exception as e:
                logger.error(f"Erro ao extrair {data_do_dia} ({nome_stat}): {e}")
                datas_com_erro.append({
                    "data": data_do_dia, "nome_status": nome_stat, 
                    "codigo_situacao": cod_sit, "tipo_periodo": tipo_per
                })

            contador_requisicoes += 1

    sessao.close()
    return {"dfs_sucesso": dfs_fase1, "erros_para_reextrair": datas_com_erro}


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def extrair_fase_reextracao(
    usuario: str, senha: str, resultados_fase1: dict, timeout_login: int, timeout_consulta: int, tempo_espera: float, max_tentativas: int
) -> dict:
    
    logger = prefect.context.get("logger")
    dfs_totais = resultados_fase1["dfs_sucesso"]
    erros_pendentes = resultados_fase1["erros_para_reextrair"]
    erros_definitivos = []
    
    if not erros_pendentes:
        logger.info("Nenhum erro reportado na Fase 1. A saltar a Fase 2.")
        return {"dfs": dfs_totais, "erros": erros_definitivos}

    logger.info(f"--- INÍCIO DA REEXTRAÇÃO DE {len(erros_pendentes)} FALHAS ---")
    sessao = criar_sessao_autenticada(usuario, senha, timeout_login)

    for erro in erros_pendentes:
        df_recuperado = _executar_com_retentativa(
            sessao, usuario, senha, erro, logger, 
            timeout_consulta, tempo_espera, timeout_login, max_tentativas
        )
        
        if df_recuperado is not None:
            if not df_recuperado.empty:
                dfs_totais.append(df_recuperado)
            else:
                logger.info(f"📭 Nenhum registo (vazio) recuperado para {erro['data']}.")
        else:
            logger.error(f"❌ Desistência em relação a {erro['data']} [{erro['nome_status']}].")
            erros_definitivos.append(f"{erro['data']} ({erro['tipo_periodo']}) - {erro['nome_status']}")

    sessao.close()
    return {"dfs": dfs_totais, "erros": erros_definitivos}


@task
def salvar_resultados(dados_extraidos: dict, dataset_id: str, table_id: str) -> None:
    logger = prefect.context.get("logger")
    dfs = dados_extraidos["dfs"]
    datas_com_erro = dados_extraidos["erros"]

    if dfs:
        dfs_traduzidos = []
        for df in dfs:
            df.rename(columns=TRADUTOR_COLUNAS_SISREG, inplace=True)
            df = df.loc[:, ~df.columns.str.contains('^unnamed|erro', case=False)]
            dfs_traduzidos.append(df)

        df_final = pd.concat(dfs_traduzidos, ignore_index=True)
        df_final['data_particao'] = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Iniciando envio para o DataLake: {dataset_id}.{table_id} ({len(df_final)} registros)")

        upload_df_to_datalake.run(
            df=df_final,
            dataset_id=dataset_id, 
            table_id=table_id,    
            partition_column='data_particao',        
            source_format="parquet",
        )
        logger.info("✅ Dados enviados para o DataLake com sucesso!")
    else:
        logger.warning("Nenhuma tabela foi extraída. O DataLake não será atualizado.")

    if datas_com_erro:
        logger.error(f"ATENÇÃO: {len(datas_com_erro)} extrações falharam definitivamente.")
        for erro in datas_com_erro:
            logger.error(f"Falha não recuperada: {erro}")