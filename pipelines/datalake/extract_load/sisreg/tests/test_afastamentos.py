# -*- coding: utf-8 -*-
"""
Testes do extrator de afastamentos (extractors/afastamentos.py).

Todos offline: sessao mockada, HTML da fixture.
CPFs nunca logados em texto (LGPD).
"""
import os
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.errors import ErroVazioSuspeito

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _html(nome: str) -> str:
    with open(os.path.join(_FIXTURE_DIR, nome), encoding="utf-8") as f:
        return f.read()


def _mock_sessao(html_atual: str, html_hist: str) -> MagicMock:
    def _resp(texto):
        r = MagicMock()
        r.status_code = 200
        r.text = texto
        r.raise_for_status = MagicMock()
        return r

    sessao = MagicMock()
    sessao.get.side_effect = [_resp(html_atual), _resp(html_hist)]
    return sessao


class TestParsearAfastamentosCurrentPage(unittest.TestCase):
    def test_parseia_html_fixture(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from pipelines.datalake.extract_load.sisreg.extractors.afastamentos import (
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

        from pipelines.datalake.extract_load.sisreg.extractors.afastamentos import (
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

        from pipelines.datalake.extract_load.sisreg.extractors.afastamentos import (
            _parsear_afastamentos_current,
        )

        html = "<html><body><table class='table_listagem'><tr><td>Titulo</td></tr></table></body></html>"
        resultado = _parsear_afastamentos_current(
            html, "11111111111", datetime.now(ZoneInfo("America/Sao_Paulo"))
        )
        self.assertIsNone(resultado)


class TestPlanejarTrabalhoAfastamentos(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.afastamentos._obter_cpfs_ativos")
    def test_retorna_um_item_por_cpf(self, mock_cpfs) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.afastamentos import (
            planejar_trabalho_afastamentos,
        )

        mock_cpfs.return_value = ["11111111111", "22222222222", "33333333333"]
        items = planejar_trabalho_afastamentos(credenciais={}, params={})
        self.assertEqual(len(items), 3)
        # CPF deve estar presente mas o id de log nao deve ser o CPF bruto
        for item in items:
            self.assertIn("cpf", item)
            self.assertIn("cpf_idx", item)
            self.assertFalse(item["cpf_idx"].startswith(item["cpf"]))

    @patch("pipelines.datalake.extract_load.sisreg.extractors.afastamentos._obter_cpfs_ativos")
    def test_lista_vazia_levanta_erro_vazio_suspeito(self, mock_cpfs) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.afastamentos import (
            planejar_trabalho_afastamentos,
        )

        mock_cpfs.side_effect = ErroVazioSuspeito("zero CPFs", conjunto="afastamentos")
        with self.assertRaises(ErroVazioSuspeito):
            planejar_trabalho_afastamentos(credenciais={}, params={})


if __name__ == "__main__":
    unittest.main()
