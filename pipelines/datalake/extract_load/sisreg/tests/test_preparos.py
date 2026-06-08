# -*- coding: utf-8 -*-
"""
Testes do extrator de preparos (extractors/preparos.py).

Todos offline: sessao mockada, HTML das fixtures.
"""
import os
import unittest
from unittest.mock import MagicMock, patch

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


class TestObterUnidades(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.preparos.requisicao_educada")
    def test_extrai_unidades_da_fixture(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.preparos import (
            _obter_unidades,
        )

        mock_req.return_value = _mock_resp(_html("preparos_unidades.html"))
        unidades = _obter_unidades(MagicMock())
        # Fixture tem 2 options (excluindo o placeholder)
        nomes = [u.get_text(strip=True) for u in unidades if u.get("value")]
        self.assertIn("UBS CENTRAL", nomes)
        self.assertIn("UBS NORTE", nomes)


class TestLimparDescricao(unittest.TestCase):
    def test_substitui_ponto_e_virgula(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.preparos import (
            _limpar_descricao,
        )

        resultado = _limpar_descricao("jejum; exames")
        self.assertNotIn(";", resultado)

    def test_texto_vazio_retorna_string_vazia(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.preparos import (
            _limpar_descricao,
        )

        self.assertEqual(_limpar_descricao(""), "")
        self.assertEqual(_limpar_descricao(None), "")


class TestPlanejarTrabalhoPreparos(unittest.TestCase):
    def test_retorna_um_item(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.preparos import (
            planejar_trabalho_preparos,
        )

        items = planejar_trabalho_preparos(credenciais={}, params={})
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], "todas_as_unidades")


if __name__ == "__main__":
    unittest.main()
