# -*- coding: utf-8 -*-
"""
Autenticacao e sessao do SISREG.

Implementa o login canonico (GET pagina -> extrair campos ocultos -> POST com
sha256 da senha em maiusculo) conforme o flow sisreg_solicitacoes (o mais
limpo do legado). Adiciona:

- TLS sempre ligado (verify=True). Nunca desabilitar - enviamos credenciais
  governamentais. Se a cadeia de certificados estiver incompleta, pinamos o
  bundle correto via certifi, nunca desabilitamos a verificacao.
- raise_for_status() em toda resposta.
- Reautenticacao inline (reautenticar_se_deslogado): se a sessao expirar no
  meio de um loop longo, refaz o login reutilizando o mesmo cookie jar.

Nao ha failover entre contas: cada perfil de credencial usa uma unica conta
(/sisreg ou /sisreg_regulacao). Um ErroBloqueio e tratado como circuit-break
no extrator, nunca com troca de conta (o mesmo IP bloquearia a outra conta).
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
from pipelines.datalake.extract_load.sisreg.errors import (
    ErroAutenticacao,
    ErroEstrutura,
)

# Palavras que confirmam login bem-sucedido na pagina pos-login.
# Derivadas de sisreg_solicitacoes (o mais conservador).
_PALAVRAS_SUCESSO_LOGIN = ("sair", "menu", "bem-vindo", "leia com regularidade")

# Palavras que indicam sessao expirada (para reautenticacao inline).
_PALAVRAS_SESSAO_EXPIRADA = (
    "efetue o logon novamente",
    "sua sessao foi finalizada",
    "sessao expirada",
)

# Frases que indicam que a conta AUTENTICOU mas o SISREG esta forcando uma troca
# obrigatoria de senha (senha expirada). Essa pagina contem "menu"/"sair" e
# passaria no teste de sucesso (falso-positivo), entao precisa ser detectada
# explicitamente e tratada como falha de autenticacao - exige acao humana
# (resetar a senha da conta no SISREG e atualizar o segredo no Infisical).
# Confirmado no spike EPIC 11: a conta /sisreg_regulacao caiu nesta pagina.
_PALAVRAS_SENHA_EXPIRADA = (
    "senha expirada",
    "informe uma nova senha",
)


def _detectar_senha_expirada(html: str) -> bool:
    """Retorna True se a pagina e a tela de troca obrigatoria de senha."""
    texto_lower = html.lower()
    return any(p in texto_lower for p in _PALAVRAS_SENHA_EXPIRADA)


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


def _parece_pagina_login(html: str) -> bool:
    """Retorna True se a resposta e a pagina de login REEXIBIDA.

    Um login bem-sucedido redireciona para fora da pagina de login. Se a resposta
    ainda traz um campo de senha ou o widget reCAPTCHA, a autenticacao NAO passou
    (credencial invalida ou desafio de captcha) - mesmo que a pagina contenha
    "menu"/"sair". Detectado explicitamente para nao reportar sucesso falso.
    Confirmado no spike EPIC 11: senha errada reexibe o login com reCAPTCHA.
    """
    if "recaptcha" in html.lower():
        return True
    soup = BeautifulSoup(html, "html.parser")
    return bool(soup.find("input", {"type": "password"}))


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

    # A pagina de troca obrigatoria de senha contem "menu"/"sair" e passaria no
    # teste de sucesso. Detectar antes e falhar loud: nenhuma extracao funciona
    # ate a senha ser trocada manualmente no SISREG.
    if _detectar_senha_expirada(r_post.text):
        raise ErroAutenticacao(
            "Senha da conta SISREG expirada - troca obrigatoria necessaria (acao humana)",
            conjunto=conjunto,
            etapa="login",
            url=url_post,
            detalhe="pagina de troca obrigatoria de senha apos login",
        )

    # Pagina de login reexibida (campo de senha/reCAPTCHA) = credencial invalida.
    if _parece_pagina_login(r_post.text):
        raise ErroAutenticacao(
            "Login falhou: pagina de login reexibida (credencial invalida ou reCAPTCHA)",
            conjunto=conjunto,
            etapa="login",
            url=url_post,
            detalhe="campo de senha ou reCAPTCHA presente apos POST de login",
        )

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

    if _detectar_senha_expirada(r_post.text):
        raise ErroAutenticacao(
            "Senha da conta SISREG expirada - troca obrigatoria necessaria (acao humana)",
            conjunto=conjunto,
            etapa="reautenticacao",
            url=url_post,
            detalhe="pagina de troca obrigatoria de senha apos reautenticacao",
        )

    if _parece_pagina_login(r_post.text):
        raise ErroAutenticacao(
            "Reautenticacao falhou: pagina de login reexibida (credencial invalida ou reCAPTCHA)",
            conjunto=conjunto,
            etapa="reautenticacao",
            url=url_post,
            detalhe="campo de senha ou reCAPTCHA presente apos POST de reautenticacao",
        )

    if not _verificar_sucesso_login(r_post.text):
        raise ErroAutenticacao(
            "Reautenticacao falhou",
            conjunto=conjunto,
            etapa="reautenticacao",
            url=url_post,
        )

    log(f"[{conjunto}] Reautenticacao bem-sucedida")
    return True
