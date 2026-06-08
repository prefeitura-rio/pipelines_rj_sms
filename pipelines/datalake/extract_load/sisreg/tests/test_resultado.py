# -*- coding: utf-8 -*-
"""
Testes dos contratos de resultado (resultado.py).

Verifica a propriedade incompleto do ResultadoConjunto e a estrutura
do Consolidado. Todos offline - nenhuma dependencia externa.
"""
import unittest

import pandas as pd

from pipelines.datalake.extract_load.sisreg.resultado import Consolidado, ResultadoConjunto


class TestResultadoConjunto(unittest.TestCase):
    """Testa a propriedade incompleto e a construcao do dataclass."""

    def test_todos_ok_nao_incompleto(self) -> None:
        r = ResultadoConjunto(tabelas={}, total=5, ok=5)
        self.assertFalse(r.incompleto)

    def test_ok_menor_que_total_e_incompleto(self) -> None:
        r = ResultadoConjunto(tabelas={}, total=5, ok=4)
        self.assertTrue(r.incompleto)

    def test_ids_falhos_nao_vazio_e_incompleto(self) -> None:
        r = ResultadoConjunto(tabelas={}, total=5, ok=5, ids_falhos=["cpf_1"])
        self.assertTrue(r.incompleto)

    def test_zero_sub_itens_ok_nao_incompleto(self) -> None:
        """total=0, ok=0 e tecnicamente completo (sem itens para processar)."""
        r = ResultadoConjunto(tabelas={}, total=0, ok=0)
        self.assertFalse(r.incompleto)

    def test_ids_falhos_default_vazio(self) -> None:
        r = ResultadoConjunto(tabelas={}, total=1, ok=1)
        self.assertEqual(r.ids_falhos, [])

    def test_tabelas_acessiveis(self) -> None:
        df = pd.DataFrame({"col": [1, 2]})
        r = ResultadoConjunto(tabelas={"tb": df}, total=1, ok=1)
        self.assertIn("tb", r.tabelas)
        self.assertEqual(len(r.tabelas["tb"]), 2)


class TestConsolidado(unittest.TestCase):
    """Testa a estrutura do dataclass Consolidado."""

    def test_tabelas_none_representa_falha(self) -> None:
        c = Consolidado(tabelas=None, metricas={"status": "FALHA"})
        self.assertIsNone(c.tabelas)
        self.assertEqual(c.metricas["status"], "FALHA")

    def test_tabelas_presentes_representa_sucesso(self) -> None:
        df = pd.DataFrame({"col": [1]})
        c = Consolidado(tabelas={"tb": df}, metricas={"status": "OK", "items_total": 1})
        self.assertIsNotNone(c.tabelas)
        self.assertIn("tb", c.tabelas)

    def test_metricas_dict_livre(self) -> None:
        """metricas e um dict livre - qualquer chave deve ser aceita."""
        c = Consolidado(tabelas=None, metricas={"items_total": 10, "items_ok": 8})
        self.assertEqual(c.metricas["items_total"], 10)


if __name__ == "__main__":
    unittest.main()
