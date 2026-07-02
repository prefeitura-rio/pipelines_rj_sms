# -*- coding: utf-8 -*-
"""
Testes do extrator de afastamentos (extractors/afastamentos.py).

Todos offline: sessao mockada, HTML da fixture.
CPFs nunca logados em texto (LGPD).
"""
import os
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.nucleo.errors import (
    ErroBloqueio,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.nucleo.resultado import ResultadoConjunto

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _html(nome: str) -> str:
    with open(os.path.join(_FIXTURE_DIR, nome), encoding="utf-8") as f:
        return f.read()


def _mock_resp(texto: str) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = texto
    r.raise_for_status = MagicMock()
    return r


class TestParsearAfastamentosCurrentPage(unittest.TestCase):
    def test_parseia_html_fixture(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            _parsear_afastamentos_current,
        )

        html = _html("afastamentos.html")
        df = _parsear_afastamentos_current(
            html, "12345678901", datetime.now(ZoneInfo("America/Sao_Paulo"))
        )
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 0)
        self.assertIn("cpf", df.columns)

    def test_profissional_nao_encontrado_retorna_none(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            _parsear_afastamentos_current,
        )

        html = "<html><body>Profissional nao encontrado!</body></html>"
        resultado = _parsear_afastamentos_current(
            html, "99999999999", datetime.now(ZoneInfo("America/Sao_Paulo"))
        )
        self.assertIsNone(resultado)

    def test_sem_tabela_retorna_none(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            _parsear_afastamentos_current,
        )

        html = (
            "<html><body>"
            "<table class='table_listagem'><tr><td>Titulo</td></tr></table>"
            "</body></html>"
        )
        resultado = _parsear_afastamentos_current(
            html, "11111111111", datetime.now(ZoneInfo("America/Sao_Paulo"))
        )
        self.assertIsNone(resultado)


class TestPlanejarTrabalhoAfastamentos(unittest.TestCase):
    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos._obter_cpfs_ativos"
    )
    def test_retorna_item_como_dict_com_cpfs(self, mock_cpfs) -> None:
        """Apos o coarsening, planejar retorna 1 dict com todos os CPFs."""
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            planejar_trabalho_afastamentos,
        )

        mock_cpfs.return_value = ["11111111111", "22222222222", "33333333333"]
        item = planejar_trabalho_afastamentos(credenciais={}, params={})
        self.assertIsInstance(item, dict)
        self.assertIn("cpfs", item)
        self.assertEqual(len(item["cpfs"]), 3)
        self.assertEqual(item["id"], "todos_os_cpfs")

    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos._obter_cpfs_ativos"
    )
    def test_lista_vazia_levanta_erro_vazio_suspeito(self, mock_cpfs) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            planejar_trabalho_afastamentos,
        )

        mock_cpfs.side_effect = ErroVazioSuspeito("zero CPFs", conjunto="afastamentos")
        with self.assertRaises(ErroVazioSuspeito):
            planejar_trabalho_afastamentos(credenciais={}, params={})


class TestExtrairItemAfastamentos(unittest.TestCase):
    """Testa extrair_item_afastamentos com sessao unica e multiplos CPFs."""

    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors"
        ".afastamentos.requisicao_com_reauth"
    )
    def test_loop_usa_sessao_unica(self, mock_req) -> None:
        """Confirma que a sessao e reusada para todos os CPFs (anti-ban)."""
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            extrair_item_afastamentos,
        )

        html_af = _html("afastamentos.html")
        html_hist = "<html><body>Nenhum Log Encontrado</body></html>"
        mock_req.return_value = _mock_resp(html_af)
        # Alterna: af, hist, af, hist para 2 CPFs
        mock_req.side_effect = [
            _mock_resp(html_af),
            _mock_resp(html_hist),
            _mock_resp(html_af),
            _mock_resp(html_hist),
        ]
        sessao = MagicMock()

        item = {"id": "todos_os_cpfs", "cpfs": ["11111111111", "22222222222"]}
        resultado = extrair_item_afastamentos(sessao=sessao, item=item, params={})

        self.assertIsInstance(resultado, ResultadoConjunto)
        self.assertEqual(resultado.total, 2)
        self.assertEqual(resultado.ok, 2)
        self.assertEqual(resultado.ids_falhos, [])
        self.assertIn("afastamentos", resultado.tabelas)
        self.assertIn("afastamento_historico", resultado.tabelas)

    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors"
        ".afastamentos.requisicao_com_reauth"
    )
    def test_bloqueio_ip_aborta_run(self, mock_req) -> None:
        """ErroBloqueio (IP) deve propagar imediatamente."""
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            extrair_item_afastamentos,
        )

        mock_req.side_effect = ErroBloqueio(
            "403 bloqueado", conjunto="afastamentos", detalhe="HTTP_403"
        )
        item = {"id": "todos_os_cpfs", "cpfs": ["11111111111"]}
        with self.assertRaises(ErroBloqueio):
            extrair_item_afastamentos(sessao=MagicMock(), item=item, params={})

    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors"
        ".afastamentos.requisicao_com_reauth"
    )
    def test_erro_por_cpf_vai_para_ids_falhos(self, mock_req) -> None:
        """Erros recuperaveis de um CPF nao abortam o loop - vao para ids_falhos."""
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            extrair_item_afastamentos,
        )

        html_af = _html("afastamentos.html")
        html_hist = "<html><body>Nenhum Log Encontrado</body></html>"
        # Primeiro CPF falha com erro generico; segundo CPF ok
        mock_req.side_effect = [
            Exception("timeout de rede"),
            _mock_resp(html_af),
            _mock_resp(html_hist),
        ]
        item = {"id": "todos_os_cpfs", "cpfs": ["11111111111", "22222222222"]}
        resultado = extrair_item_afastamentos(sessao=MagicMock(), item=item, params={})

        self.assertEqual(resultado.total, 2)
        self.assertEqual(resultado.ok, 1)
        self.assertEqual(len(resultado.ids_falhos), 1)
        self.assertEqual(resultado.ids_falhos[0], "cpf_0")  # primeiro CPF

    # Patches no modulo auth: o extrator usa o requisicao_com_reauth real,
    # exercitando o caminho completo logout -> reauth -> retry.
    @patch("pipelines.datalake.extract_load.sisreg.nucleo.auth.reautenticar_se_deslogado")
    @patch("pipelines.datalake.extract_load.sisreg.nucleo.auth.requisicao_educada")
    def test_logout_mid_run_reautentica(self, mock_req, mock_reauth) -> None:
        """REDIRECIONAMENTO_LOGIN dispara reauth; a sessao continua."""
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.afastamentos import (
            extrair_item_afastamentos,
        )

        html_af = _html("afastamentos.html")
        html_hist = "<html><body>Nenhum Log Encontrado</body></html>"
        logout_exc = ErroBloqueio(
            "logout", conjunto="afastamentos", detalhe="REDIRECIONAMENTO_LOGIN"
        )
        mock_req.side_effect = [
            logout_exc,  # 1a tentativa: sessao expirada
            _mock_resp(html_af),  # apos reauth: ok
            _mock_resp(html_hist),
        ]
        mock_reauth.return_value = True

        item = {"id": "todos_os_cpfs", "cpfs": ["11111111111"]}
        resultado = extrair_item_afastamentos(
            sessao=MagicMock(), item=item, params={"usuario": "u", "senha": "p"}
        )

        mock_reauth.assert_called_once()
        self.assertEqual(resultado.ok, 1)
        self.assertEqual(resultado.ids_falhos, [])


if __name__ == "__main__":
    unittest.main()
