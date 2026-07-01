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
    def test_retorna_item_com_shard(self) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.preparos import (
            planejar_trabalho_preparos,
        )

        item = planejar_trabalho_preparos(credenciais={}, params={"num_shards_preparos": 7})
        self.assertIsInstance(item, dict)
        self.assertEqual(item["id"], "lote_de_unidades")
        self.assertEqual(item["num_shards"], 7)
        self.assertIn("shard", item)
        self.assertTrue(0 <= item["shard"] < 7)


class _FakeOption:
    """Simula uma tag <option> (get('value') e get_text())."""

    def __init__(self, valor: str) -> None:
        self._valor = valor

    def get(self, chave, default=""):
        return self._valor if chave == "value" else default

    def get_text(self, strip: bool = False) -> str:
        return f"unidade_{self._valor}"


class TestExtrairItemPreparosSharding(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.preparos._obter_procedimentos")
    @patch("pipelines.datalake.extract_load.sisreg.extractors.preparos._obter_unidades")
    def test_processa_apenas_as_unidades_do_shard(self, mock_unidades, mock_procs) -> None:
        """Com num_shards=3 e shard=0, so as unidades de indice 0 e 3 sao processadas."""
        from pipelines.datalake.extract_load.sisreg.extractors.preparos import (
            extrair_item_preparos,
        )

        mock_unidades.return_value = [_FakeOption(str(i)) for i in range(6)]
        mock_procs.return_value = []  # sem procedimentos -> nenhum GET de preparo

        extrair_item_preparos(sessao=MagicMock(), item={"shard": 0, "num_shards": 3}, params={})

        valores = [chamada.args[1] for chamada in mock_procs.call_args_list]
        self.assertEqual(valores, ["0", "3"])


if __name__ == "__main__":
    unittest.main()
