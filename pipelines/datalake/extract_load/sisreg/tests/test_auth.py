# -*- coding: utf-8 -*-
"""
Testes de autenticacao e failover de conta (common/auth.py).

Todos offline: requests.Session e mockada, nenhuma chamada real ao SISREG.
"""
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.common.auth import (
    _extrair_action_formulario,
    _verificar_sucesso_login,
    extrair_campos_ocultos,
    reautenticar_se_deslogado,
    sha256_maiusculo,
)
from pipelines.datalake.extract_load.sisreg.errors import (
    ErroAutenticacao,
    ErroEstrutura,
)

# ---------------------------------------------------------------------------
# HTML minimo para testes de parsing
# ---------------------------------------------------------------------------

_HTML_LOGIN_OK = """
<html><body>
<form action="/cgi-bin/index">
  <input type="hidden" name="token" value="abc123">
  <input type="hidden" name="state" value="xyz">
  <input type="text" name="usuario">
</form>
</body></html>
"""

_HTML_LOGIN_OK_ABSOLUTE = """
<html><body>
<form action="https://sisregiii.saude.gov.br/cgi-bin/index">
  <input type="hidden" name="token" value="tok1">
</form>
</body></html>
"""

_HTML_LOGIN_SEM_FORM = "<html><body><p>nada aqui</p></body></html>"

_HTML_POS_LOGIN_SUCESSO = "<html><body>Bem-vindo ao SISREG - <a>sair</a></body></html>"
_HTML_POS_LOGIN_FALHA = "<html><body>Senha incorreta</body></html>"
_HTML_SESSAO_EXPIRADA = "<html><body>Efetue o logon novamente</body></html>"


# ---------------------------------------------------------------------------
# Helpers de mock
# ---------------------------------------------------------------------------


def _mock_resp(text: str = "", status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------


class TestSha256Maiusculo(unittest.TestCase):
    def test_hash_determinista(self) -> None:
        h1 = sha256_maiusculo("senha123")
        h2 = sha256_maiusculo("senha123")
        self.assertEqual(h1, h2)

    def test_equivale_maiusculo(self) -> None:
        """sha256('senha'.upper()) deve ser igual a sha256_maiusculo('SENHA')."""
        self.assertEqual(sha256_maiusculo("senha"), sha256_maiusculo("SENHA"))

    def test_comprimento_hex(self) -> None:
        self.assertEqual(len(sha256_maiusculo("x")), 64)


class TestExtrairCamposOcultos(unittest.TestCase):
    def test_extrai_campos(self) -> None:
        campos = extrair_campos_ocultos(_HTML_LOGIN_OK)
        self.assertEqual(campos["token"], "abc123")
        self.assertEqual(campos["state"], "xyz")

    def test_ignora_campos_sem_name(self) -> None:
        html = '<input type="hidden" value="sem-name">'
        self.assertEqual(extrair_campos_ocultos(html), {})

    def test_html_sem_campos_retorna_dict_vazio(self) -> None:
        self.assertEqual(extrair_campos_ocultos("<html></html>"), {})


class TestExtrairActionFormulario(unittest.TestCase):
    def test_action_relativo(self) -> None:
        url = _extrair_action_formulario(_HTML_LOGIN_OK, "https://sisregiii.saude.gov.br")
        self.assertTrue(url.startswith("https://sisregiii.saude.gov.br"))

    def test_action_absoluto(self) -> None:
        url = _extrair_action_formulario(_HTML_LOGIN_OK_ABSOLUTE, "https://sisregiii.saude.gov.br")
        self.assertEqual(url, "https://sisregiii.saude.gov.br/cgi-bin/index")

    def test_sem_form_levanta_erro_estrutura(self) -> None:
        with self.assertRaises(ErroEstrutura):
            _extrair_action_formulario(_HTML_LOGIN_SEM_FORM, "https://x.com")


class TestVerificarSucessoLogin(unittest.TestCase):
    def test_pagina_com_sair_e_sucesso(self) -> None:
        self.assertTrue(_verificar_sucesso_login(_HTML_POS_LOGIN_SUCESSO))

    def test_pagina_sem_indicadores_e_falha(self) -> None:
        self.assertFalse(_verificar_sucesso_login(_HTML_POS_LOGIN_FALHA))


class TestAbrirSessaoAutenticada(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.common.auth.requests.Session")
    def test_login_bem_sucedido_retorna_sessao(self, mock_session_cls) -> None:
        from pipelines.datalake.extract_load.sisreg.common.auth import (
            abrir_sessao_autenticada,
        )

        sessao_mock = MagicMock()
        mock_session_cls.return_value = sessao_mock
        sessao_mock.get.return_value = _mock_resp(_HTML_LOGIN_OK)
        sessao_mock.post.return_value = _mock_resp(_HTML_POS_LOGIN_SUCESSO)

        resultado = abrir_sessao_autenticada("user", "senha", conjunto="escalas")
        self.assertIs(resultado, sessao_mock)

    @patch("pipelines.datalake.extract_load.sisreg.common.auth.requests.Session")
    def test_login_com_falha_levanta_erro_autenticacao(self, mock_session_cls) -> None:
        from pipelines.datalake.extract_load.sisreg.common.auth import (
            abrir_sessao_autenticada,
        )

        sessao_mock = MagicMock()
        mock_session_cls.return_value = sessao_mock
        sessao_mock.get.return_value = _mock_resp(_HTML_LOGIN_OK)
        sessao_mock.post.return_value = _mock_resp(_HTML_POS_LOGIN_FALHA)

        with self.assertRaises(ErroAutenticacao):
            abrir_sessao_autenticada("user", "senhaerrada")


class TestReautenticarSeDeslogado(unittest.TestCase):
    def test_sessao_valida_retorna_false(self) -> None:
        sessao = MagicMock()
        resultado = reautenticar_se_deslogado(sessao, "pagina normal", "u", "p")
        self.assertFalse(resultado)
        sessao.get.assert_not_called()

    @patch("pipelines.datalake.extract_load.sisreg.common.auth.requests.Session")
    def test_sessao_expirada_reautentica(self, _) -> None:
        sessao = MagicMock()
        sessao.get.return_value = _mock_resp(_HTML_LOGIN_OK)
        sessao.post.return_value = _mock_resp(_HTML_POS_LOGIN_SUCESSO)

        resultado = reautenticar_se_deslogado(sessao, _HTML_SESSAO_EXPIRADA, "user", "senha")
        self.assertTrue(resultado)


if __name__ == "__main__":
    unittest.main()
