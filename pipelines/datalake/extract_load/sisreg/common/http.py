# -*- coding: utf-8 -*-
"""
Camada HTTP educada do SISREG.

Centraliza todo acesso de rede: sleep com jitter, deteccao de bloqueio
(CAPTCHA, 403, 429, redirecionamento de logout), backoff exponencial e
captura de snapshot HTML para diagnostico.

Principio: nao ha tentativa de burlar o CAPTCHA. Qualquer sinal de bloqueio
aciona circuit-break imediato - grinding em cima de um bloqueio e o caminho
mais rapido para um ban permanente.
"""

import time
from random import uniform
from typing import Optional

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.constants import (
    DELAY_MAXIMO_S,
    DELAY_MINIMO_S,
    FATOR_BACKOFF,
    MAX_TENTATIVAS_ITEM,
    TIMEOUT_CONSULTA_S,
)
from pipelines.datalake.extract_load.sisreg.errors import ErroBloqueio, ErroTransitorio

# Marcadores textuais que indicam bloqueio/CAPTCHA na resposta do SISREG.
# Derivados dos flows legados (afastamentos detecta "(CAPTCHA)").
_MARCADORES_CAPTCHA = ["(CAPTCHA)", "captcha", "robot", "acesso negado"]
_MARCADORES_LOGOUT = [
    "efetue o logon novamente",
    "sua sessao foi finalizada",
    "sessao expirada",
]


def detectar_bloqueio(resposta: requests.Response, conjunto: str = "") -> Optional[str]:
    """Verifica se a resposta indica bloqueio de acesso.

    Retorna o tipo de problema ("CAPTCHA", "HTTP_403", "HTTP_429",
    "REDIRECIONAMENTO_LOGIN") ou None se a resposta estiver ok.

    O chamador deve tratar o retorno nao-None como ErroBloqueio - esta funcao
    apenas classifica, nao levanta excecao, para facilitar o teste.
    """
    if resposta.status_code == 403:
        return "HTTP_403"
    if resposta.status_code == 429:
        return "HTTP_429"

    texto_lower = resposta.text.lower()

    # Resposta com status 200 mas redirecionada para a pagina de login
    if any(m in texto_lower for m in _MARCADORES_LOGOUT):
        return "REDIRECIONAMENTO_LOGIN"

    if any(m in texto_lower for m in _MARCADORES_CAPTCHA):
        return "CAPTCHA"

    return None


def _dormir_com_jitter(
    delay_min: float = DELAY_MINIMO_S, delay_max: float = DELAY_MAXIMO_S
) -> None:
    """Dorme um intervalo aleatorio entre delay_min e delay_max segundos.

    O jitter evita periocidade perfeita que identifica scrapers. Nunca
    usar sleep(valor_fixo) - use sempre esta funcao.
    """
    duracao = uniform(delay_min, delay_max)
    log(f"Aguardando {duracao:.2f}s antes da proxima requisicao")
    time.sleep(duracao)


def requisicao_educada(
    sessao: requests.Session,
    url: str,
    params: Optional[dict] = None,
    conjunto: str = "",
    item: str = "",
    tentativas: int = MAX_TENTATIVAS_ITEM,
    delay_min: float = DELAY_MINIMO_S,
    delay_max: float = DELAY_MAXIMO_S,
    timeout: float = TIMEOUT_CONSULTA_S,
) -> requests.Response:
    """Executa GET com politeness: sleep jitter, retry com backoff, deteccao de bloqueio.

    Levanta ErroBloqueio imediatamente ao detectar CAPTCHA/403/429/logout
    (sem retry - grinding piora o bloqueio). Levanta ErroTransitorio para
    falhas de rede dentro do limite de tentativas.

    Args:
        sessao: Sessao requests autenticada.
        url: URL de destino.
        params: Parametros de query string.
        conjunto: Chave do conjunto para contexto de erro.
        item: Identificador do item para contexto de erro.
        tentativas: Numero maximo de tentativas para erros transitorios.
        delay_min: Limite inferior do jitter em segundos.
        delay_max: Limite superior do jitter em segundos.
        timeout: Timeout da requisicao em segundos.
    """
    _dormir_com_jitter(delay_min, delay_max)

    ultimo_erro: Optional[Exception] = None
    for tentativa in range(1, tentativas + 1):
        try:
            resposta = sessao.get(url, params=params, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            ultimo_erro = exc
            if tentativa < tentativas:
                espera = DELAY_MINIMO_S * (FATOR_BACKOFF ** (tentativa - 1))
                log(
                    f"[{conjunto}/{item}] tentativa {tentativa}/{tentativas} falhou "
                    f"({type(exc).__name__}). Aguardando {espera:.1f}s"
                )
                time.sleep(espera)
            continue

        # Verificar bloqueio ANTES de raise_for_status: 403/429 sao bloqueio,
        # nao erro transitorio - não retry.
        tipo_bloqueio = detectar_bloqueio(resposta, conjunto=conjunto)
        if tipo_bloqueio:
            raise ErroBloqueio(
                f"Acesso bloqueado: {tipo_bloqueio}",
                conjunto=conjunto,
                etapa="requisicao",
                item=item,
                url=url,
                detalhe=tipo_bloqueio,
            )

        resposta.raise_for_status()
        return resposta

    # Esgotou as tentativas - erro transitorio irrecuperavel para este item
    raise ErroTransitorio(
        f"Falha apos {tentativas} tentativas",
        conjunto=conjunto,
        etapa="requisicao",
        item=item,
        url=url,
        detalhe=str(ultimo_erro),
    )
