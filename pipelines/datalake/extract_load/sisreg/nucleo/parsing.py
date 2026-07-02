# -*- coding: utf-8 -*-
"""
Utilidades de parsing de HTML para o SISREG.

Converte as respostas HTML do SISREG em DataFrames pandas, normaliza nomes
de colunas para snake_case ASCII (compativel com BigQuery) e valida landmarks
DOM antes de tentar parsear - qualquer landmark ausente levanta ErroEstrutura
imediatamente, tornando mudancas de layout detectaveis como erros nomeados.
"""

import os
import re
import unicodedata
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.nucleo.errors import ErroEstrutura


def normalizar_nome_coluna(nome: str) -> str:
    """Converte um nome de coluna para snake_case ASCII sem acentos.

    Utiliza a mesma logica de sisreg_solicitacoes: normaliza NFKD, remove
    acentos, substitui caracteres nao-alfanumericos por underscore e strip.
    """
    # Remove acentos via decomposicao Unicode
    nfkd = unicodedata.normalize("NFKD", nome)
    ascii_str = nfkd.encode("ascii", errors="ignore").decode("utf-8")
    # Substitui qualquer sequencia nao alfanumerica por underscore
    snaked = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_str)
    return snaked.strip("_").lower()


def normalizar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica normalizar_nome_coluna a todas as colunas do DataFrame.

    Garante unicidade: colunas duplicadas apos normalizacao recebem sufixo
    numerico (_1, _2, ...) para evitar sobrescrita silenciosa.
    """
    vistos: set = set()
    novas: list = []
    for col in df.columns:
        novo = normalizar_nome_coluna(str(col))
        if novo in vistos:
            contador = 1
            candidato = f"{novo}_{contador}"
            while candidato in vistos:
                contador += 1
                candidato = f"{novo}_{contador}"
            novo = candidato
        vistos.add(novo)
        novas.append(novo)
    df.columns = novas
    return df


def _extrair_cor_do_src(src: str) -> str:
    """Extrai o nome do arquivo (sem extensao) do atributo src de uma imagem.

    Usado para converter imagens de risco (verde.png, amarelo.png) em strings.
    """
    return os.path.splitext(os.path.basename(src))[0]


def tabela_listagem_para_dataframe(
    html: str,
    colunas_obrigatorias: Optional[tuple] = None,
    conjunto: str = "",
    item: str = "",
) -> pd.DataFrame:
    """Extrai a primeira table.table_listagem valida do HTML e retorna DataFrame.

    A linha 0 do <tbody> e um titulo/colgroup; a linha 1 contem os cabecalhos;
    as linhas 2+ sao dados - padrao consistente em todos os flows legados.

    Colunas com imagens (ex.: Risco) sao convertidas para o nome do arquivo
    sem extensao (ex.: "verde", "amarelo").

    Args:
        html: Texto HTML da pagina do SISREG.
        colunas_obrigatorias: Tupla de strings que devem estar nos cabecalhos;
            levanta ErroEstrutura se alguma estiver ausente.
        conjunto: Contexto para mensagens de erro.
        item: Contexto para mensagens de erro.

    Returns:
        DataFrame com colunas normalizadas; vazio se nenhuma linha de dados.
    """
    soup = BeautifulSoup(html, "lxml")
    tabelas = soup.find_all("table", class_="table_listagem")

    if not tabelas:
        raise ErroEstrutura(
            "table.table_listagem nao encontrada no HTML",
            conjunto=conjunto,
            etapa="parsing",
            item=item,
            detalhe="Landmark ausente - provavel mudanca no layout do SISREG",
        )

    # Procura a primeira tabela que contenha as colunas obrigatorias
    tabela_alvo = None
    for tb in tabelas:
        rows = tb.find_all("tr")
        if len(rows) < 2:
            continue
        texto_tb = tb.get_text().lower()
        if colunas_obrigatorias and not all(c.lower() in texto_tb for c in colunas_obrigatorias):
            continue
        tabela_alvo = tb
        break

    if tabela_alvo is None:
        raise ErroEstrutura(
            "Nenhuma table_listagem contem as colunas obrigatorias",
            conjunto=conjunto,
            etapa="parsing",
            item=item,
            detalhe=str(colunas_obrigatorias),
        )

    rows = tabela_alvo.find_all("tr")
    # Linha 1 contem os cabecalhos (linha 0 e titulo/colgroup)
    cabecalhos = [cel.get_text(strip=True) for cel in rows[1].find_all(["td", "th"])]

    dados = []
    for row in rows[2:]:
        celulas = row.find_all("td")
        if not celulas:
            continue
        linha = []
        for i, cel in enumerate(celulas):
            # Imagens de risco: extrair nome do arquivo como valor
            img = cel.find("img")
            if img and img.get("src"):
                linha.append(_extrair_cor_do_src(img["src"]))
            else:
                linha.append(cel.get_text(strip=True))
        dados.append(linha)

    if not dados:
        log(f"[{conjunto}/{item}] table_listagem encontrada mas sem linhas de dados")
        return pd.DataFrame(columns=cabecalhos)

    df = pd.DataFrame(dados, columns=cabecalhos)
    return normalizar_nomes_colunas(df)
