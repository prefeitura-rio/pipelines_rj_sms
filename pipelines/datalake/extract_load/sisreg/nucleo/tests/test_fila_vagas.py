# -*- coding: utf-8 -*-
"""
Testes do extrator de fila e vagas (extractors/fila_vagas.py).

Todos offline: sessao mockada, HTML das fixtures ou inline.
"""
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.nucleo.errors import (
    ErroBloqueio,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.nucleo.resultado import ResultadoConjunto


def _mock_resp(texto: str) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = texto
    r.raise_for_status = MagicMock()
    return r


def _proc(id_sisreg: str = "P001") -> dict:
    return {
        "id_procedimento_grupo": "G1",
        "procedimento_grupo": "Cardiologia",
        "id_procedimento_sisreg": id_sisreg,
        "procedimento": "Consulta A",
    }


class TestPlanejarTrabalhoFilaVagas(unittest.TestCase):
    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors"
        ".fila_vagas._obter_procedimentos_bq"
    )
    def test_retorna_item_como_dict_com_procedimentos(self, mock_bq) -> None:
        """Apos o coarsening, planejar retorna 1 dict com todos os procedimentos."""
        import pandas as pd

        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas import (
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
        item = planejar_trabalho_fila_vagas(credenciais={}, params={})
        self.assertIsInstance(item, dict)
        self.assertIn("procedimentos", item)
        self.assertEqual(item["id"], "procedimentos")
        self.assertEqual(len(item["procedimentos"]), 2)

    @patch(
        "pipelines.datalake.extract_load.sisreg.nucleo.extractors"
        ".fila_vagas._obter_procedimentos_bq"
    )
    def test_bq_vazio_levanta_erro(self, mock_bq) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas import (
            planejar_trabalho_fila_vagas,
        )

        mock_bq.side_effect = ErroVazioSuspeito("vazio", conjunto="fila_vagas")
        with self.assertRaises(ErroVazioSuspeito):
            planejar_trabalho_fila_vagas(credenciais={}, params={})


class TestExtrairItemFilaVagas(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas.requisicao_educada")
    def test_inexistentes_retorna_dataframes_com_nulos(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas import (
            extrair_item_fila_vagas,
        )

        mock_req.return_value = _mock_resp("<html><body>INEXISTENTES!</body></html>")
        item = {"id": "procedimentos", "procedimentos": [_proc("P999")]}
        resultado = extrair_item_fila_vagas(sessao=MagicMock(), item=item, params={})

        self.assertIsInstance(resultado, ResultadoConjunto)
        self.assertIn("fila_e_vagas", resultado.tabelas)
        self.assertEqual(len(resultado.tabelas["fila_e_vagas"]), 1)
        self.assertIsNone(resultado.tabelas["fila_e_vagas"]["qtd_pend"].iloc[0])
        self.assertEqual(resultado.ok, 1)

    @patch("pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas.requisicao_educada")
    def test_nenhuma_vaga_encontrada_retorna_zero_vagas(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas import (
            extrair_item_fila_vagas,
        )

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
        html_aplicar = "<html><body>NENHUMA VAGA ENCONTRADA</body></html>"
        mock_req.side_effect = [_mock_resp(html_listar), _mock_resp(html_aplicar)]

        item = {"id": "procedimentos", "procedimentos": [_proc("P001")]}
        resultado = extrair_item_fila_vagas(sessao=MagicMock(), item=item, params={})

        self.assertIn("fila_e_vagas", resultado.tabelas)
        self.assertEqual(resultado.tabelas["fila_e_vagas"]["qtd_vagas"].iloc[0], 0)
        self.assertEqual(resultado.ok, 1)

    @patch("pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas.requisicao_educada")
    def test_bloqueio_propaga(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas import (
            extrair_item_fila_vagas,
        )

        mock_req.side_effect = ErroBloqueio("403", conjunto="fila_vagas", detalhe="HTTP_403")
        item = {"id": "procedimentos", "procedimentos": [_proc("P001")]}
        with self.assertRaises(ErroBloqueio):
            extrair_item_fila_vagas(sessao=MagicMock(), item=item, params={})

    @patch("pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas.requisicao_educada")
    def test_erro_por_procedimento_vai_para_ids_falhos(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.extractors.fila_vagas import (
            extrair_item_fila_vagas,
        )

        html_listar = "<html><body>INEXISTENTES!</body></html>"
        # P001 falha com erro generico; P002 retorna INEXISTENTES (ok)
        mock_req.side_effect = [
            Exception("timeout"),
            _mock_resp(html_listar),
        ]
        item = {
            "id": "procedimentos",
            "procedimentos": [_proc("P001"), _proc("P002")],
        }
        resultado = extrair_item_fila_vagas(sessao=MagicMock(), item=item, params={})

        self.assertEqual(resultado.total, 2)
        self.assertEqual(resultado.ok, 1)
        self.assertIn("P001", resultado.ids_falhos)


if __name__ == "__main__":
    unittest.main()
