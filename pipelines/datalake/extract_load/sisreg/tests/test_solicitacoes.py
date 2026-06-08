# -*- coding: utf-8 -*-
"""
Testes do extrator de solicitacoes (extractors/solicitacoes.py).

Todos offline: sessao mockada, HTML da fixture.
"""
import os
import unittest
from unittest.mock import MagicMock, patch

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _html(nome: str) -> str:
    with open(os.path.join(_FIXTURE_DIR, nome), encoding="utf-8") as f:
        return f.read()


class TestGerarRoteiro(unittest.TestCase):
    def test_roteiro_contem_todas_as_combinacoes(self) -> None:
        from pipelines.datalake.extract_load.sisreg.constants import (
            CONFIGS_SOLICITACOES,
        )
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            _gerar_roteiro,
        )

        roteiro = _gerar_roteiro(janela_dias=2)
        # 3 dias (hoje, ontem, anteontem) x len(CONFIGS_SOLICITACOES) status
        n_status = len(CONFIGS_SOLICITACOES)
        self.assertEqual(len(roteiro), 3 * n_status)

    def test_item_tem_campos_obrigatorios(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            _gerar_roteiro,
        )

        roteiro = _gerar_roteiro(janela_dias=1)
        for item in roteiro:
            self.assertIn("data", item)
            self.assertIn("tipo_pedido", item)
            self.assertIn("codigo_situacao", item)
            self.assertIn("status_coluna", item)


class TestVerificarStatusPagina(unittest.TestCase):
    def test_logout_detectado(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            _verificar_status_pagina,
        )

        self.assertEqual(
            _verificar_status_pagina("Efetue o logon novamente"),
            "LOGOUT",
        )

    def test_vazio_detectado(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            _verificar_status_pagina,
        )

        self.assertEqual(
            _verificar_status_pagina("Nenhum registro encontrado"),
            "VAZIO",
        )

    def test_ok_detectado(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            _verificar_status_pagina,
        )

        self.assertEqual(
            _verificar_status_pagina("pagina normal com dados"),
            "OK",
        )


class TestExtrairItemSolicitacoes(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.solicitacoes.requisicao_educada")
    def test_parseia_fixture_retorna_dataframe(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            extrair_item_solicitacoes,
        )

        resp = MagicMock()
        resp.text = _html("solicitacoes.html")
        mock_req.return_value = resp

        item = {
            "data": "01/06/2025",
            "tipo_pedido": "S",
            "codigo_situacao": "1",
            "status_coluna": "PENDENTE",
            "id": "01/06/2025_PENDENTE",
        }
        resultado = extrair_item_solicitacoes(sessao=MagicMock(), item=item, params={})
        self.assertIn("solicitacoes", resultado)
        self.assertGreater(len(resultado["solicitacoes"]), 0)

    @patch("pipelines.datalake.extract_load.sisreg.extractors.solicitacoes.requisicao_educada")
    def test_vazio_retorna_dataframe_vazio(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            extrair_item_solicitacoes,
        )

        resp = MagicMock()
        resp.text = "<html><body>Nenhum registro encontrado</body></html>"
        mock_req.return_value = resp

        item = {
            "data": "01/06/2025",
            "tipo_pedido": "S",
            "codigo_situacao": "1",
            "status_coluna": "PENDENTE",
        }
        resultado = extrair_item_solicitacoes(sessao=MagicMock(), item=item, params={})
        self.assertIn("solicitacoes", resultado)
        self.assertTrue(resultado["solicitacoes"].empty)


if __name__ == "__main__":
    unittest.main()
