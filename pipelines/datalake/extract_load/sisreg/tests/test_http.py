# -*- coding: utf-8 -*-
"""
Testes da camada HTTP educada (common/http.py).

Todos os testes sao offline: requests.Session e mockada, nenhuma chamada
real ao SISREG e feita.
"""
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.common.http import (
    detectar_bloqueio,
    requisicao_educada,
)
from pipelines.datalake.extract_load.sisreg.errors import ErroBloqueio, ErroTransitorio


def _mock_response(status_code: int = 200, text: str = "ok") -> MagicMock:
    """Cria um mock de requests.Response com os campos necessarios."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        import requests

        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    return resp


class TestDetectarBloqueio(unittest.TestCase):
    """Testa a classificacao de bloqueio sem fazer requisicoes."""

    def test_resposta_normal_retorna_none(self) -> None:
        resp = _mock_response(200, "bem-vindo ao sisreg")
        self.assertIsNone(detectar_bloqueio(resp))

    def test_status_403(self) -> None:
        resp = _mock_response(403)
        self.assertEqual(detectar_bloqueio(resp), "HTTP_403")

    def test_status_429(self) -> None:
        resp = _mock_response(429)
        self.assertEqual(detectar_bloqueio(resp), "HTTP_429")

    def test_captcha_no_texto(self) -> None:
        resp = _mock_response(200, "pagina com (CAPTCHA) obrigatorio")
        self.assertEqual(detectar_bloqueio(resp), "CAPTCHA")

    def test_pagina_autenticada_com_meta_robots_nao_e_bloqueio(self) -> None:
        """Regressao (spike EPIC 11): toda pagina autenticada do SISREG carrega
        <meta name="robots">. O token "robot" NAO pode ser tratado como CAPTCHA,
        senao toda requisicao em producao abortaria com falso-positivo.
        """
        html = (
            "<html><head><title>SISREG III - Servidor de Producao</title>"
            '<meta name="robots" content="noindex,follow" /></head>'
            "<body>menu ... sair</body></html>"
        )
        resp = _mock_response(200, html)
        self.assertIsNone(detectar_bloqueio(resp))

    def test_palavra_captcha_sem_parenteses_nao_e_bloqueio(self) -> None:
        """A forma nua "captcha" (ex.: nome de arquivo JS) nao deve disparar bloqueio;
        somente a forma parentetica "(CAPTCHA)" do desafio real conta.
        """
        resp = _mock_response(200, '<script src="/js/captcha_helper.js"></script> menu')
        self.assertIsNone(detectar_bloqueio(resp))

    def test_redirecionamento_logout(self) -> None:
        resp = _mock_response(200, "Efetue o logon novamente para continuar")
        self.assertEqual(detectar_bloqueio(resp), "REDIRECIONAMENTO_LOGIN")

    def test_sessao_expirada(self) -> None:
        resp = _mock_response(200, "Sua sessao foi finalizada pelo administrador")
        self.assertEqual(detectar_bloqueio(resp), "REDIRECIONAMENTO_LOGIN")


class TestRequisicaoEducada(unittest.TestCase):
    """Testa o comportamento de requisicao_educada com sessao mockada."""

    def _sessao_com_resposta(self, resposta: MagicMock) -> MagicMock:
        sessao = MagicMock()
        sessao.get.return_value = resposta
        return sessao

    @patch("pipelines.datalake.extract_load.sisreg.common.http._dormir_com_jitter")
    def test_sucesso_retorna_resposta(self, mock_sleep) -> None:
        resp = _mock_response(200, "sair")
        sessao = self._sessao_com_resposta(resp)
        resultado = requisicao_educada(sessao, "http://example.com", tentativas=1)
        self.assertEqual(resultado, resp)

    @patch("pipelines.datalake.extract_load.sisreg.common.http._dormir_com_jitter")
    @patch("time.sleep")
    def test_captcha_levanta_erro_bloqueio(self, mock_ts, mock_sleep) -> None:
        resp = _mock_response(200, "pagina (CAPTCHA) bloqueada")
        sessao = self._sessao_com_resposta(resp)
        with self.assertRaises(ErroBloqueio) as ctx:
            requisicao_educada(sessao, "http://example.com", conjunto="escalas")
        self.assertIn("CAPTCHA", str(ctx.exception))

    @patch("pipelines.datalake.extract_load.sisreg.common.http._dormir_com_jitter")
    @patch("time.sleep")
    def test_falha_rede_com_retry_levanta_erro_transitorio(self, mock_ts, mock_sleep) -> None:
        import requests as req_lib

        sessao = MagicMock()
        sessao.get.side_effect = req_lib.exceptions.ConnectionError("sem rede")
        with self.assertRaises(ErroTransitorio) as ctx:
            requisicao_educada(sessao, "http://example.com", tentativas=2, conjunto="x")
        self.assertIn("2 tentativas", str(ctx.exception))
        self.assertEqual(sessao.get.call_count, 2)

    @patch("pipelines.datalake.extract_load.sisreg.common.http._dormir_com_jitter")
    @patch("time.sleep")
    def test_retry_bem_sucedido_na_segunda_tentativa(self, mock_ts, mock_sleep) -> None:
        import requests as req_lib

        resp_ok = _mock_response(200, "menu")
        sessao = MagicMock()
        sessao.get.side_effect = [req_lib.exceptions.Timeout("lento"), resp_ok]
        resultado = requisicao_educada(sessao, "http://example.com", tentativas=2)
        self.assertEqual(resultado, resp_ok)
        self.assertEqual(sessao.get.call_count, 2)

    @patch("pipelines.datalake.extract_load.sisreg.common.http._dormir_com_jitter")
    def test_bloqueio_nao_faz_retry(self, mock_sleep) -> None:
        """ErroBloqueio deve ser levantado na primeira ocorrencia, sem retry."""
        resp = _mock_response(403)
        sessao = self._sessao_com_resposta(resp)
        with self.assertRaises(ErroBloqueio):
            requisicao_educada(sessao, "http://example.com", tentativas=3)
        # Nao deve ter tentado mais de uma vez
        self.assertEqual(sessao.get.call_count, 1)


class TestJitterBounds(unittest.TestCase):
    """Testa que o jitter permanece dentro dos limites configurados."""

    def test_jitter_dentro_dos_limites(self) -> None:
        from pipelines.datalake.extract_load.sisreg.common.http import (
            _dormir_com_jitter,
        )

        duracao_medida: list[float] = []

        def sleep_captura(n: float) -> None:
            duracao_medida.append(n)

        with patch("time.sleep", side_effect=sleep_captura):
            for _ in range(20):
                _dormir_com_jitter(delay_min=1.0, delay_max=2.0)

        for d in duracao_medida:
            self.assertGreaterEqual(d, 1.0)
            self.assertLessEqual(d, 2.0)


if __name__ == "__main__":
    unittest.main()
