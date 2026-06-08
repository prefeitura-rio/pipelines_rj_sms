# -*- coding: utf-8 -*-
"""
Testes do extrator de fila e vagas (extractors/fila_vagas.py).

Todos offline: sessao mockada, HTML das fixtures ou inline.
"""
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.errors import ErroVazioSuspeito


def _mock_resp(texto: str) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = texto
    r.raise_for_status = MagicMock()
    return r


class TestPlanejarTrabalhoFilaVagas(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.fila_vagas._obter_procedimentos_bq")
    def test_retorna_um_item_por_procedimento(self, mock_bq) -> None:
        import pandas as pd

        from pipelines.datalake.extract_load.sisreg.extractors.fila_vagas import (
            planejar_trabalho_fila_vagas,
        )

        mock_bq.return_value = pd.DataFrame(
            {
                "id_procedimento_grupo": ["G1", "G1"],
                "procedimento_grupo": ["Cardiologia", "Cardiologia"],
                "id_procedimento_sisreg": ["P001", "P002"],
                "procedimento": ["Consulta A", "Consulta B"],
            }
        )
        items = planejar_trabalho_fila_vagas(credenciais={}, params={})
        self.assertEqual(len(items), 2)
        for item in items:
            self.assertIn("id_procedimento_sisreg", item)
            self.assertIn("id", item)
            # id deve ser o codigo, nao o nome
            self.assertIn(item["id"], ["P001", "P002"])

    @patch("pipelines.datalake.extract_load.sisreg.extractors.fila_vagas._obter_procedimentos_bq")
    def test_bq_vazio_levanta_erro(self, mock_bq) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.fila_vagas import (
            planejar_trabalho_fila_vagas,
        )

        mock_bq.side_effect = ErroVazioSuspeito("vazio", conjunto="fila_vagas")
        with self.assertRaises(ErroVazioSuspeito):
            planejar_trabalho_fila_vagas(credenciais={}, params={})


class TestExtrairItemInexistente(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.fila_vagas.requisicao_educada")
    def test_inexistentes_retorna_dataframes_com_nulos(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.fila_vagas import (
            extrair_item_fila_vagas,
        )

        mock_req.return_value = _mock_resp("<html><body>INEXISTENTES!</body></html>")
        item = {
            "id_procedimento_sisreg": "P999",
            "id_procedimento_grupo": "G1",
            "procedimento_grupo": "X",
            "procedimento": "Y",
        }
        resultado = extrair_item_fila_vagas(sessao=MagicMock(), item=item, params={})
        self.assertIn("fila_e_vagas", resultado)
        self.assertIn("vagas_detalhadas", resultado)
        # fila_e_vagas deve ter 1 linha com qtd_pend=None
        self.assertEqual(len(resultado["fila_e_vagas"]), 1)
        self.assertIsNone(resultado["fila_e_vagas"]["qtd_pend"].iloc[0])

    @patch("pipelines.datalake.extract_load.sisreg.extractors.fila_vagas.requisicao_educada")
    def test_nenhuma_vaga_encontrada_retorna_zero_vagas(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.fila_vagas import (
            extrair_item_fila_vagas,
        )

        # Primeira chamada: LISTAR
        html_listar = """
        <html><body>
        <form>
        <table><tr><td></td></tr></table>
        <table><tr><td></td></tr></table>
        <table><tr><td></td></tr><tr><td></td></tr>
          <tr><td></td><td>SOL-001</td></tr>
        </table>
        <td class="td_titulo_tabela">X</td>
        <td class="td_titulo_tabela">Y</td>
        <td class="td_titulo_tabela">Fila: 3</td>
        </form>
        </body></html>
        """
        # Segunda chamada: APLICAR sem vagas
        html_aplicar = "<html><body>NENHUMA VAGA ENCONTRADA</body></html>"
        mock_req.side_effect = [_mock_resp(html_listar), _mock_resp(html_aplicar)]

        item = {
            "id_procedimento_sisreg": "P001",
            "id_procedimento_grupo": "G1",
            "procedimento_grupo": "X",
            "procedimento": "Y",
        }
        resultado = extrair_item_fila_vagas(sessao=MagicMock(), item=item, params={})
        self.assertIn("fila_e_vagas", resultado)
        # qtd_vagas = 0 quando nenhuma vaga encontrada
        self.assertEqual(resultado["fila_e_vagas"]["qtd_vagas"].iloc[0], 0)


if __name__ == "__main__":
    unittest.main()
