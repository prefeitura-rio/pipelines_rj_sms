# -*- coding: utf-8 -*-
"""
Testes do extrator de solicitacoes (extractors/solicitacoes.py).

Todos offline: sessao mockada, HTML da fixture.
"""
import os
import unittest
from unittest.mock import MagicMock, patch

from pipelines.datalake.extract_load.sisreg.errors import ErroBloqueio
from pipelines.datalake.extract_load.sisreg.resultado import ResultadoConjunto

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


def _ficha(data: str = "01/06/2025", status_coluna: str = "PENDENTE") -> dict:
    return {
        "data": data,
        "tipo_pedido": "S",
        "codigo_situacao": "1",
        "status_coluna": status_coluna,
        "id": f"{data}_{status_coluna}",
    }


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


class TestPlanejarTrabalhoSolicitacoes(unittest.TestCase):
    def test_retorna_item_como_dict_com_roteiro(self) -> None:
        """Apos o coarsening, planejar retorna 1 dict com o roteiro completo."""
        from pipelines.datalake.extract_load.sisreg.constants import (
            CONFIGS_SOLICITACOES,
        )
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            planejar_trabalho_solicitacoes,
        )

        item = planejar_trabalho_solicitacoes(credenciais={}, params={"janela_dias": 2})
        self.assertIsInstance(item, dict)
        self.assertIn("roteiro", item)
        self.assertEqual(item["id"], "roteiro")
        # janela=2 -> 3 dias x 7 status = 21 fichas
        self.assertEqual(len(item["roteiro"]), 3 * len(CONFIGS_SOLICITACOES))


class TestExtrairItemSolicitacoes(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.extractors.solicitacoes.requisicao_educada")
    def test_parseia_fixture_retorna_resultado_conjunto(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            extrair_item_solicitacoes,
        )

        mock_req.return_value = _mock_resp(_html("solicitacoes.html"))
        item = {"id": "roteiro", "roteiro": [_ficha()]}
        resultado = extrair_item_solicitacoes(sessao=MagicMock(), item=item, params={})

        self.assertIsInstance(resultado, ResultadoConjunto)
        self.assertIn("solicitacoes", resultado.tabelas)
        self.assertGreater(len(resultado.tabelas["solicitacoes"]), 0)
        self.assertEqual(resultado.total, 1)
        self.assertEqual(resultado.ok, 1)

    @patch("pipelines.datalake.extract_load.sisreg.extractors.solicitacoes.requisicao_educada")
    def test_vazio_retorna_df_vazio_ok_um(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            extrair_item_solicitacoes,
        )

        mock_req.return_value = _mock_resp("<html><body>Nenhum registro encontrado</body></html>")
        item = {"id": "roteiro", "roteiro": [_ficha()]}
        resultado = extrair_item_solicitacoes(sessao=MagicMock(), item=item, params={})

        self.assertIsInstance(resultado, ResultadoConjunto)
        self.assertTrue(resultado.tabelas["solicitacoes"].empty)
        # VAZIO conta como ok (nao e um erro, data nao tinha registros)
        self.assertEqual(resultado.ok, 1)

    @patch("pipelines.datalake.extract_load.sisreg.extractors.solicitacoes.requisicao_educada")
    def test_bloqueio_propaga(self, mock_req) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            extrair_item_solicitacoes,
        )

        mock_req.side_effect = ErroBloqueio("403", conjunto="solicitacoes", detalhe="HTTP_403")
        item = {"id": "roteiro", "roteiro": [_ficha()]}
        with self.assertRaises(ErroBloqueio):
            extrair_item_solicitacoes(sessao=MagicMock(), item=item, params={})

    @patch(
        "pipelines.datalake.extract_load.sisreg.extractors.solicitacoes"
        ".reautenticar_se_deslogado"
    )
    @patch("pipelines.datalake.extract_load.sisreg.extractors.solicitacoes.requisicao_educada")
    def test_logout_mid_run_reautentica(self, mock_req, mock_reauth) -> None:
        from pipelines.datalake.extract_load.sisreg.extractors.solicitacoes import (
            extrair_item_solicitacoes,
        )

        logout_exc = ErroBloqueio(
            "logout", conjunto="solicitacoes", detalhe="REDIRECIONAMENTO_LOGIN"
        )
        mock_req.side_effect = [
            logout_exc,
            _mock_resp("<html><body>Nenhum registro encontrado</body></html>"),
        ]
        mock_reauth.return_value = True
        item = {"id": "roteiro", "roteiro": [_ficha()]}
        resultado = extrair_item_solicitacoes(
            sessao=MagicMock(), item=item, params={"usuario": "u", "senha": "p"}
        )
        mock_reauth.assert_called_once()
        self.assertEqual(resultado.ok, 1)


if __name__ == "__main__":
    unittest.main()
