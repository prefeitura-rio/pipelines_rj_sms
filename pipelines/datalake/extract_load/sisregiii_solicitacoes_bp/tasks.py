# -*- coding: utf-8 -*-

import os
import time
import hashlib
import calendar
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
    USER_AGENT
)

# Funções de tratamento, detecção de html, login, verificações e extrações.
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


def realizar_login(sessao: requests.Session, usuario: str, senha: str) -> bool:
    logger = prefect.context.get("logger")
    r_get = sessao.get(BASE_URL, timeout=30)
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
    
    r_post = sessao.post(url_post_real, data=payload, allow_redirects=True, timeout=30)
    texto_pagina = r_post.text.lower()
    
    if "sair" in texto_pagina or "menu" in texto_pagina or "bem-vindo" in texto_pagina:
        logger.info("Login realizado com sucesso.")
        return True
    else:
        raise FAIL("Falha no login do Sisreg.")


def criar_sessao_autenticada(usuario: str, senha: str) -> requests.Session:
    # Desativa avisos de SSL inseguro no console
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    HEADERS_DISFARCE = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1"
    }

    sessao = requests.Session()
    sessao.headers.update(HEADERS_DISFARCE)
    sessao.verify = False  
    
    realizar_login(sessao, usuario, senha)
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


def _processar_pagina_sisreg(sessao: requests.Session, usuario: str, senha: str, data_req: str, periodo_req: str, status_req: str, cod_situacao_req: str, logger: logging.Logger) -> pd.DataFrame:
    url_consulta = (
        f"{GERENCIADOR_URL}?etapa=LISTAR_SOLICITACOES"
        f"&co_solicitacao=&cns_paciente=&no_usuario=&cnes_solicitante=&cnes_executante=&co_proc_unificado="
        f"&co_pa_interno=&ds_procedimento=&tipo_periodo={periodo_req}"
        f"&dt_inicial={data_req}&dt_final={data_req}"
        f"&cmb_situacao={cod_situacao_req}&qtd_itens_pag=0&co_seq_solicitacao=&ordenacao=2&pagina=0"
    )

    resposta = sessao.get(url_consulta, timeout=180)
    time.sleep(8.5)
    
    status_pagina = _verificar_resposta_html(resposta.text)

    if status_pagina == "SESSAO_ZUMBI":
        logger.warning(
            f"Sessão zumbi detectada. Relogando para {data_req}..."
        )
        realizar_login(sessao, usuario, senha)
        resposta = sessao.get(url_consulta, timeout=180)
        status_pagina = _verificar_resposta_html(resposta.text)

    if status_pagina == "VAZIO":
        return pd.DataFrame()

    soup = BeautifulSoup(resposta.text, 'html.parser')
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


# Tasks
@task
def gerar_roteiro_extracao(data_especifica: str | None = None) -> dict:
    logger = prefect.context.get("logger")
    
    if data_especifica:
        logger.info(f"Modo de teste ativado: A extrair apenas a data {data_especifica}")
        datas = [data_especifica]
    else:
        hoje = datetime.now()
        mes_anterior = hoje.replace(day=1) - timedelta(days=1)
        ano, mes = mes_anterior.year, mes_anterior.month
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        datas = [(f"{dia:02d}/{mes:02d}/{ano}") for dia in range(1, ultimo_dia + 1)]
        logger.info(f"Foram geradas {len(datas)} datas para a extração do mês {mes:02d}/{ano}.")

    STATUS_SISREG = {
        "PENDENTE": "1", "CANCELADO": "3", "DEVOLVIDO": "4",
        "REENVIADO": "5", "NEGADO": "6", "APROVADO": "7", "CANCELADO2": "10"
    }

    configs_extracao = [
        {"tipo_periodo": "A", "codigo_situacao": "7", "status_coluna": "AUTORIZADO"},
        {"tipo_periodo": "E", "codigo_situacao": "7", "status_coluna": "EXECUTADO"},
    ]
    
    for nome, cod in STATUS_SISREG.items():
        configs_extracao.append({
            "tipo_periodo": "S", "codigo_situacao": cod,
            "status_coluna": "CANCELADO" if nome == "CANCELADO2" else nome
        })

    return {"datas": datas, "configs": configs_extracao}


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def extrair_fase_principal(usuario: str, senha: str, roteiro: dict) -> dict:
    logger = prefect.context.get("logger")
    datas, configs_extracao = roteiro["datas"], roteiro["configs"]
    
    dfs_fase1 = []
    datas_com_erro = []
    contador_requisicoes = 0
    
    sessao = criar_sessao_autenticada(usuario, senha)

    for data_do_dia in datas:
        for config in configs_extracao:
            tipo_per, cod_sit, nome_stat = config["tipo_periodo"], config["codigo_situacao"], config["status_coluna"]
            
            if contador_requisicoes > 0 and contador_requisicoes % 15 == 0:
                logger.info("A renovar a sessão proativamente...")
                sessao.close()
                sessao = criar_sessao_autenticada(usuario, senha)

            logger.info(f"[FASE 1] Extração: {data_do_dia} | Período: {tipo_per} | Status: {nome_stat}")
            
            try:
                df = _processar_pagina_sisreg(sessao, usuario, senha, data_do_dia, tipo_per, nome_stat, cod_sit, logger)
                
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
def extrair_fase_reextracao(usuario: str, senha: str, resultados_fase1: dict) -> dict:
    logger = prefect.context.get("logger")
    dfs_totais = resultados_fase1["dfs_sucesso"]
    erros_pendentes = resultados_fase1["erros_para_reextrair"]
    erros_definitivos = []
    
    if not erros_pendentes:
        logger.info("Nenhum erro reportado na Fase 1. A saltar a Fase 2.")
        return {"dfs": dfs_totais, "erros": erros_definitivos}

    logger.info(
        f"--- INÍCIO DA REEXTRAÇÃO DE {len(erros_pendentes)} FALHAS ---"
    )

    MAX_TENTATIVAS_REEXTRA = 100 
    
    sessao = criar_sessao_autenticada(usuario, senha)

    for erro in erros_pendentes:
        data_erro, nome_erro, cod_erro, per_erro = erro["data"], erro["nome_status"], erro["codigo_situacao"], erro["tipo_periodo"]
        sucesso_reextra = False
        
        for tentativa in range(1, MAX_TENTATIVAS_REEXTRA + 1):
            logger.info(f"🔄 [FASE 2] Tentativa {tentativa}/{MAX_TENTATIVAS_REEXTRA} -> {data_erro} [{per_erro}-{nome_erro}]")
            
            try:
                df = _processar_pagina_sisreg(sessao, usuario, senha, data_erro, per_erro, nome_erro, cod_erro, logger)
                
                if not df.empty:
                    dfs_totais.append(df)
                else:
                    logger.info(f"📭 Nenhum registo (vazio) para {data_erro}.")
                    
                logger.info(f"✅ Sucesso na reextração de {data_erro}!")
                sucesso_reextra = True
                break
                
            except Exception as e:
                logger.error(f"Falha na tentativa {tentativa}: {e}")
                time.sleep(10)
        
        if not sucesso_reextra:
            logger.error(f"❌ Desistência em relação a {data_erro} [{nome_erro}].")
            erros_definitivos.append(f"{data_erro} ({per_erro}) - {nome_erro}")

    sessao.close()
    return {"dfs": dfs_totais, "erros": erros_definitivos}


@task
def salvar_resultados(dados_extraidos: dict, status_desejado: str) -> None:
    logger = prefect.context.get("logger")
    dfs = dados_extraidos["dfs"]
    datas_com_erro = dados_extraidos["erros"]

    if dfs:
        tradutor_colunas = {
            'cod_solicitac': 'cod_solicitacao', 
            'cod_solicitacao': 'cod_solicitacao',
            'data': 'data_da_solicitacao', 
            'data_solicitacao': 'data_da_solicitacao',
            'municipio_residencia': 'municipio', 
            'municipio': 'municipio',
            'situacao_atual': 'situacao', 
            'sit': 'situacao',
            'situacao': 'situacao',
            'data_execucao': 'data_da_execucao', 
            'dt_exec': 'data_da_execucao',
            'data_autorizacao': 'data_da_autorizacao', 
            'dt_aut': 'data_da_autorizacao'
        }
        
        dfs_traduzidos = []
        for df in dfs:
            df.rename(columns=tradutor_colunas, inplace=True)
            df = df.loc[:, ~df.columns.str.contains('^unnamed|erro', case=False)]
            dfs_traduzidos.append(df)

        df_final = pd.concat(dfs_traduzidos, ignore_index=True)
        df_final['data_particao'] = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(
            f"Iniciando envio para o DataLake... ({len(df_final)} registros totais extraídos)"
        )

        upload_df_to_datalake.run(
            df=df_final,
            dataset_id='brutos_sisreg_solicitacoes', 
            table_id='tb_sisreg_solicitacoes_bp',    
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