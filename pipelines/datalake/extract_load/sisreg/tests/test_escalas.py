# -*- coding: utf-8 -*-
"""
Testes do extrator de escalas (extractors/escalas.py).

Todos offline: sessao requests mockada, CSV parseado da fixture sintetica.
"""
import os
import unittest
from unittest.mock import MagicMock

from pipelines.datalake.extract_load.sisreg.errors import ErroVazioSuspeito

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _csv_bytes(nome: str) -> bytes:
    with open(os.path.join(_FIXTURE_DIR, nome), "rb") as f:
        return f.read()


def _mock_sessao(conteudo: bytes, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.content = conteudo
    resp.text = conteudo.decode("utf-8", errors="replace")
    resp.raise_for_status = MagicMock()
    resp.url = "https://sisregiii.saude.gov.br/cgi-bin/cons_escalas?etapa=EXPORTAR_ESCALAS"
    sessao = MagicMock()
    sessao.get.return_value = resp
    return sessao


class TestExtrairItemEscalas(unittest.TestCase):
    def test_parseia_csv_fixture(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.escalas import (
            extrair_item_escalas,
        )

        sessao = _mock_sessao(_csv_bytes("escalas.csv"))
        resultado = extrair_item_escalas(sessao=sessao, item={"ibge": "330455"}, params={})
        self.assertIn("escalas", resultado)
        df = resultado["escalas"]
        self.assertEqual(len(df), 3)

    def test_resposta_vazia_levanta_erro(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.escalas import (
            extrair_item_escalas,
        )

        sessao = _mock_sessao(b"")
        with self.assertRaises(ErroVazioSuspeito):
            extrair_item_escalas(sessao=sessao, item={}, params={})

    def test_csv_sem_linhas_levanta_erro(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.escalas import (
            extrair_item_escalas,
        )

        sessao = _mock_sessao(b"Col1;Col2\n")
        with self.assertRaises(ErroVazioSuspeito):
            extrair_item_escalas(sessao=sessao, item={}, params={})


class TestPlanejarTrabalhoEscalas(unittest.TestCase):
    def test_retorna_exatamente_um_item(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.escalas import (
            planejar_trabalho_escalas,
        )

        items = planejar_trabalho_escalas(credenciais={}, params={})
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "exportacao_completa")


class TestRegistroNoRegistry(unittest.TestCase):
    def test_extrator_registrado(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors import (  # noqa: F401
            escalas,
        )
        from pipelines.datalake.extract_load.sisreg.extractors.escalas import (
            extrair_item_escalas,
            planejar_trabalho_escalas,
        )
        from pipelines.datalake.extract_load.sisreg.registry import CONJUNTOS

        conj = CONJUNTOS["escalas"]
        self.assertIs(conj.planejar_trabalho, planejar_trabalho_escalas)
        self.assertIs(conj.extrair_item, extrair_item_escalas)


if __name__ == "__main__":
    unittest.main()
