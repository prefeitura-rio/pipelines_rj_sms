# -*- coding: utf-8 -*-
"""
Testes dos helpers de parsing HTML (common/parsing.py).

Todos offline: usam fixtures sinteticas em tests/fixtures/ ou strings inline.
"""
import os
import unittest

import pandas as pd

from pipelines.datalake.extract_load.sisreg.common.parsing import (
    assertar_landmark,
    normalizar_nome_coluna,
    normalizar_nomes_colunas,
    tabela_listagem_para_dataframe,
)
from pipelines.datalake.extract_load.sisreg.errors import ErroEstrutura

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _ler_fixture(nome: str) -> str:
    with open(os.path.join(_FIXTURE_DIR, nome), encoding="utf-8") as f:
        return f.read()


class TestNormalizarNomeColuna(unittest.TestCase):
    def test_acento_removido(self) -> None:
        self.assertEqual(normalizar_nome_coluna("Código"), "codigo")

    def test_espaco_vira_underscore(self) -> None:
        self.assertEqual(normalizar_nome_coluna("Data da Solicitação"), "data_da_solicitacao")

    def test_maiusculo_vira_minusculo(self) -> None:
        self.assertEqual(normalizar_nome_coluna("NOME"), "nome")

    def test_caracteres_especiais(self) -> None:
        self.assertEqual(normalizar_nome_coluna("Cód. Solicitação"), "cod_solicitacao")

    def test_strip_underscore_bordas(self) -> None:
        resultado = normalizar_nome_coluna("  Nome  ")
        self.assertFalse(resultado.startswith("_"))
        self.assertFalse(resultado.endswith("_"))


class TestNormalizarNomesColunas(unittest.TestCase):
    def test_normaliza_todas_as_colunas(self) -> None:
        df = pd.DataFrame(columns=["Código", "Data da Solicitação", "Risco"])
        df = normalizar_nomes_colunas(df)
        self.assertListEqual(list(df.columns), ["codigo", "data_da_solicitacao", "risco"])

    def test_duplicatas_recebem_sufixo(self) -> None:
        df = pd.DataFrame(columns=["Nome", "nome"])
        df = normalizar_nomes_colunas(df)
        cols = list(df.columns)
        self.assertEqual(len(set(cols)), 2)
        self.assertIn("nome", cols)
        self.assertIn("nome_1", cols)


class TestTabelaListagemParaDataframe(unittest.TestCase):
    def test_parseia_fixture(self) -> None:
        html = _ler_fixture("table_listagem.html")
        df = tabela_listagem_para_dataframe(html)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_colunas_normalizadas(self) -> None:
        html = _ler_fixture("table_listagem.html")
        df = tabela_listagem_para_dataframe(html)
        for col in df.columns:
            self.assertEqual(col, col.lower())
            self.assertNotIn(" ", col)

    def test_imagem_risco_convertida_para_nome_arquivo(self) -> None:
        html = _ler_fixture("table_listagem.html")
        df = tabela_listagem_para_dataframe(html)
        # A coluna risco deve ter o nome do arquivo sem extensao
        risco_col = [c for c in df.columns if "risco" in c][0]
        valores = df[risco_col].tolist()
        self.assertIn("verde", valores)
        self.assertIn("amarelo", valores)

    def test_html_sem_tabela_levanta_erro_estrutura(self) -> None:
        with self.assertRaises(ErroEstrutura):
            tabela_listagem_para_dataframe("<html><body>sem tabela</body></html>")

    def test_coluna_obrigatoria_ausente_levanta_erro_estrutura(self) -> None:
        html = _ler_fixture("table_listagem.html")
        with self.assertRaises(ErroEstrutura):
            tabela_listagem_para_dataframe(html, colunas_obrigatorias=("coluna_inexistente",))

    def test_tabela_com_coluna_obrigatoria_e_encontrada(self) -> None:
        html = _ler_fixture("table_listagem.html")
        # "Risco" esta na fixture
        df = tabela_listagem_para_dataframe(html, colunas_obrigatorias=("Risco",))
        self.assertGreater(len(df), 0)


class TestAssertarLandmark(unittest.TestCase):
    _HTML = """
    <html><body>
      <textarea id="preparo">instrucoes</textarea>
      <div class="tabela-resultado">conteudo</div>
    </body></html>
    """

    def test_elemento_presente_por_id(self) -> None:
        # Nao deve levantar excecao
        assertar_landmark(self._HTML, id_elemento="preparo")

    def test_elemento_presente_por_seletor(self) -> None:
        assertar_landmark(self._HTML, seletor_css="textarea#preparo")

    def test_elemento_ausente_levanta_erro_estrutura(self) -> None:
        with self.assertRaises(ErroEstrutura) as ctx:
            assertar_landmark(self._HTML, id_elemento="elemento_inexistente", conjunto="preparos")
        self.assertIn("preparos", str(ctx.exception))

    def test_seletor_ausente_levanta_erro_estrutura(self) -> None:
        with self.assertRaises(ErroEstrutura):
            assertar_landmark(self._HTML, seletor_css="div#nao-existe")


if __name__ == "__main__":
    unittest.main()
