# -*- coding: utf-8 -*-
"""
Autenticacao, sessao e failover de conta do SISREG.

Implementa o login canonico (GET pagina -> extrair campos ocultos -> POST com
sha256 da senha em maiusculo) conforme o flow sisreg_solicitacoes (o mais
limpo do legado). Adiciona:

- TLS sempre ligado (verify=True). Nunca desabilitar - enviamos credenciais
  governamentais. Se a cadeia de certificados estiver incompleta, pinamos o
  bundle correto via certifi, nunca desabilitamos a verificacao.
- raise_for_status() em toda resposta.
- Failover gateado por categoria: apenas ErroAutenticacao (problema de conta)
  justifica rotacao; ErroBloqueio (problema de IP) nao deve rotacionar - o
  mesmo IP bloquearia a segunda conta tambem.
"""

import hashlib

import requests
from bs4 import BeautifulSoup
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.constants import (
    CABECALHOS_HTTP,
    TIMEOUT_LOGIN_S,
    URL_BASE,
)
from pipelines.datalake.extract_load.sisreg.errors import ErroAutenticacao, ErroEstrutura

# Palavras que confirmam login bem-sucedido na pagina pos-login.
# Derivadas de sisreg_solicitacoes (o mais conservador).
_PALAVRAS_SUCESSO_LOGIN = ("sair", "menu", "bem-vindo", "leia com regularidade")

# Palavras que indicam sessao expirada (para reautenticacao inline).
_PALAVRAS_SESSAO_EXPIRADA = (
    "efetue o logon novamente",
    "sua sessao foi finalizada",
    "sessao expirada",
)


def sha256_maiusculo(texto: str) -> str:
    """Calcula SHA-256 da string em maiusculo, em hex minusculo.

    O SISREG exige que a senha seja convertida para maiusculo antes do hash -
    comportamento documentado em todos os flows legados.
    """
    return hashlib.sha256(texto.upper().encode("utf-8")).hexdigest()


def extrair_campos_ocultos(html: str) -> dict:
    """Extrai todos os campos <input type="hidden"> do HTML.

    Necessario para incluir tokens anti-CSRF e campos de estado do formulario
    de login do SISREG no payload do POST.
    """
    soup = BeautifulSoup(html, "html.parser")
    campos: dict = {}
    for campo in soup.find_all("input", type="hidden"):
        nome = campo.get("name")
        if nome:
            campos[nome] = campo.get("value", "")
    return campos


def _extrair_action_formulario(html: str, url_base: str) -> str:
    """Extrai a URL de acao do primeiro formulario da pagina.

    Levanta ErroEstrutura se o formulario ou seu atributo action nao forem
    encontrados - indica mudanca no layout do SISREG.
    """
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    if not form or not form.get("action"):
        raise ErroEstrutura(
            "Formulario de login ausente ou sem action",
            etapa="login",
            detalhe="<form> ou action nao encontrados na pagina inicial",
        )
    action = form["action"]
    if action.startswith("http"):
        return action
    prefixo = "/" if not action.startswith("/") else ""
    return f"{url_base}{prefixo}{action}"


def _verificar_sucesso_login(html: str) -> bool:
    """Retorna True se o HTML pos-login contem indicadores de sessao autenticada."""
    texto_lower = html.lower()
    return any(p in texto_lower for p in _PALAVRAS_SUCESSO_LOGIN)


def abrir_sessao_autenticada(
    usuario: str,
    senha: str,
    conjunto: str = "",
) -> requests.Session:
    """Abre uma sessao requests e realiza o login no SISREG.

    Sequencia: GET pagina inicial -> extrair campos ocultos + action -> POST
    com payload completo (sha256 da senha em maiusculo) -> verificar sucesso.
    TLS sempre ligado (verify=True).

    Levanta ErroAutenticacao se o login falhar (credencial invalida, senha
    expirada). Levanta ErroEstrutura se o formulario nao for encontrado.
    """
    sessao = requests.Session()
    sessao.headers.update(CABECALHOS_HTTP)
    # TLS ligado. Se a cadeia estiver quebrada em producao, usar:
    # import certifi; sessao.verify = certifi.where()
    sessao.verify = True

    log(f"[{conjunto}] Iniciando login no SISREG como '{usuario[:3]}...'")

    r_get = sessao.get(URL_BASE, timeout=TIMEOUT_LOGIN_S)
    r_get.raise_for_status()

    url_post = _extrair_action_formulario(r_get.text, URL_BASE)
    campos_ocultos = extrair_campos_ocultos(r_get.text)

    payload = {
        **campos_ocultos,
        "usuario": usuario,
        "senha": "",
        "senha_256": sha256_maiusculo(senha),
        "etapa": "ACESSO",
        "entrar": "Entrar",
    }

    r_post = sessao.post(
        url_post,
        data=payload,
        allow_redirects=True,
        timeout=TIMEOUT_LOGIN_S,
    )
    r_post.raise_for_status()

    if not _verificar_sucesso_login(r_post.text):
        raise ErroAutenticacao(
            "Login falhou: pagina pos-login nao contem indicadores de sucesso",
            conjunto=conjunto,
            etapa="login",
            url=url_post,
            detalhe=r_post.text[:200],
        )

    log(f"[{conjunto}] Login realizado com sucesso")
    return sessao


def reautenticar_se_deslogado(
    sessao: requests.Session,
    html_resposta: str,
    usuario: str,
    senha: str,
    conjunto: str = "",
) -> bool:
    """Verifica se a sessao expirou e reautentica se necessario.

    Retorna True se a reautenticacao foi feita, False se a sessao ainda
    estava valida. Modifica a sessao in-place (reutiliza o cookie jar).

    Levanta ErroAutenticacao se a reautenticacao falhar.
    """
    texto_lower = html_resposta.lower()
    if not any(p in texto_lower for p in _PALAVRAS_SESSAO_EXPIRADA):
        return False

    log(f"[{conjunto}] Sessao expirada detectada. Reautenticando...")

    r_get = sessao.get(URL_BASE, timeout=TIMEOUT_LOGIN_S)
    r_get.raise_for_status()

    url_post = _extrair_action_formulario(r_get.text, URL_BASE)
    campos_ocultos = extrair_campos_ocultos(r_get.text)

    payload = {
        **campos_ocultos,
        "usuario": usuario,
        "senha": "",
        "senha_256": sha256_maiusculo(senha),
        "etapa": "ACESSO",
        "entrar": "Entrar",
    }

    r_post = sessao.post(
        url_post,
        data=payload,
        allow_redirects=True,
        timeout=TIMEOUT_LOGIN_S,
    )
    r_post.raise_for_status()

    if not _verificar_sucesso_login(r_post.text):
        raise ErroAutenticacao(
            "Reautenticacao falhou",
            conjunto=conjunto,
            etapa="reautenticacao",
            url=url_post,
        )

    log(f"[{conjunto}] Reautenticacao bem-sucedida")
    return True
