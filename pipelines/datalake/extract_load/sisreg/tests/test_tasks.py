# -*- coding: utf-8 -*-
"""
Testes das tasks genericas (tasks.py).

Foco: gate de completude, suspicious-empty, consolidacao e normalizacao.
Tasks que chamam Prefect internamente (resolver_credenciais, normalizar_e_subir,
registrar_log_execucao) sao testadas via .run() com mocks das dependencias
externas (get_secret_key, upload_df_to_datalake, handle_columns_to_bq).
"""
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from pipelines.datalake.extract_load.sisreg.errors import ErroVazioSuspeito


class TestObterDataExtracao(unittest.TestCase):
    """Testa o computo da data de extracao em runtime."""

    def test_retorna_formato_yyyy_mm_dd(self) -> None:
        from pipelines.datalake.extract_load.sisreg.tasks import obter_data_extracao

        resultado = obter_data_extracao.run()
        # verifica formato YYYY-MM-DD sem importar re no topo do modulo
        partes = resultado.split("-")
        self.assertEqual(len(partes), 3)
        self.assertTrue(partes[0].isdigit() and len(partes[0]) == 4)

    def test_data_nao_vazia(self) -> None:
        from pipelines.datalake.extract_load.sisreg.tasks import obter_data_extracao

        resultado = obter_data_extracao.run()
        self.assertTrue(resultado)


class TestConsolidar(unittest.TestCase):
    """Testa o gate de completude e a logica de concatenacao."""

    def _rodar(self, conjunto, fragmentos_lista, data_extracao="2025-01-01"):
        """Executa consolidar.run() com os argumentos dados."""
        from pipelines.datalake.extract_load.sisreg.tasks import consolidar

        return consolidar.run(
            conjunto=conjunto,
            fragmentos_lista=fragmentos_lista,
            data_extracao=data_extracao,
        )

    def test_100_por_cento_sucesso_retorna_dados(self) -> None:
        df = pd.DataFrame({"col": [1, 2]})
        frags = [{"escalas": df}, {"escalas": df}]
        resultado = self._rodar("escalas", frags)
        self.assertIsNotNone(resultado)
        self.assertIn("escalas", resultado)
        self.assertEqual(len(resultado["escalas"]), 4)  # 2+2 linhas

    @patch("pipelines.utils.monitor.send_message", MagicMock())
    def test_qualquer_falha_retorna_none(self) -> None:
        """Gate de completude: um None cancela o upload."""
        df = pd.DataFrame({"col": [1]})
        frags = [{"escalas": df}, None]
        resultado = self._rodar("escalas", frags)
        self.assertIsNone(resultado)

    def test_lista_vazia_retorna_none(self) -> None:
        resultado = self._rodar("preparos", [])
        self.assertIsNone(resultado)

    def test_todos_none_retorna_none(self) -> None:
        resultado = self._rodar("solicitacoes", [None, None])
        self.assertIsNone(resultado)

    def test_coluna_particao_adicionada(self) -> None:
        from pipelines.datalake.extract_load.sisreg import constants as C

        df = pd.DataFrame({"col": [1]})
        resultado = self._rodar("escalas", [{"escalas": df}], data_extracao="2025-06-01")
        self.assertIn(C.COLUNA_PARTICAO, resultado["escalas"].columns)
        self.assertEqual(resultado["escalas"][C.COLUNA_PARTICAO].iloc[0], "2025-06-01")

    def test_fragmentos_none_upstream_retorna_none(self) -> None:
        resultado = self._rodar("escalas", None)
        self.assertIsNone(resultado)


class TestPlanejarTrabalho(unittest.TestCase):
    """Testa a logica de suspicious-empty."""

    def test_lista_vazia_em_conjunto_nunca_vazio_levanta_erro(self) -> None:
        from pipelines.datalake.extract_load.sisreg.tasks import planejar_trabalho

        mock_conj = MagicMock()
        mock_conj.planejar_trabalho.return_value = []

        with patch(
            "pipelines.datalake.extract_load.sisreg.tasks.obter_conjunto",
            return_value=mock_conj,
        ):
            with self.assertRaises(ErroVazioSuspeito):
                planejar_trabalho.run(
                    conjunto="escalas",
                    credenciais={"usuario": "u", "senha": "p"},
                    params={},
                )

    def test_lista_vazia_em_preparos_nao_levanta(self) -> None:
        """preparos pode ter lista vazia sem suspeita."""
        from pipelines.datalake.extract_load.sisreg.tasks import planejar_trabalho

        mock_conj = MagicMock()
        mock_conj.planejar_trabalho.return_value = []

        with patch(
            "pipelines.datalake.extract_load.sisreg.tasks.obter_conjunto",
            return_value=mock_conj,
        ):
            # Nao deve levantar
            resultado = planejar_trabalho.run(
                conjunto="preparos",
                credenciais={},
                params={},
            )
            self.assertEqual(resultado, [])

    def test_lista_nao_vazia_retorna_lista(self) -> None:
        from pipelines.datalake.extract_load.sisreg.tasks import planejar_trabalho

        items = [{"id": "1"}, {"id": "2"}]
        mock_conj = MagicMock()
        mock_conj.planejar_trabalho.return_value = items

        with patch(
            "pipelines.datalake.extract_load.sisreg.tasks.obter_conjunto",
            return_value=mock_conj,
        ):
            resultado = planejar_trabalho.run(
                conjunto="afastamentos",
                credenciais={},
                params={},
            )
            self.assertEqual(resultado, items)


class TestNormalizarESubir(unittest.TestCase):
    """Testa a logica de normalizar_e_subir via a funcao interna (offline).

    authenticated_task exige contexto Prefect completo; testamos a logica de
    decisao (skip em None, chamada a upload em dados presentes) mockando as
    dependencias no nivel de modulo.
    """

    @patch("pipelines.datalake.extract_load.sisreg.tasks.upload_df_to_datalake")
    @patch("pipelines.datalake.extract_load.sisreg.tasks.handle_columns_to_bq")
    def test_none_pula_upload(self, mock_handle, mock_upload) -> None:
        """Com tabelas_consolidadas=None a funcao deve retornar False sem chamar upload."""
        import prefect

        df = pd.DataFrame({"col": [1]})
        mock_handle.run.return_value = df

        # authenticated_task exige prefect.context + inject_bd_credentials.
        # Patchamos no ponto de uso dentro do credential_injector.
        with patch("pipelines.utils.credential_injector.inject_bd_credentials", MagicMock()):
            with prefect.context(parameters={"environment": "staging"}, logger=MagicMock()):
                from pipelines.datalake.extract_load.sisreg.tasks import (
                    normalizar_e_subir,
                )

                resultado = normalizar_e_subir.run(
                    environment="staging",
                    conjunto="escalas",
                    dataset_id="brutos_sisreg_web",
                    tabelas_consolidadas=None,
                )
        self.assertFalse(resultado)
        mock_upload.run.assert_not_called()

    @patch("pipelines.datalake.extract_load.sisreg.tasks.upload_df_to_datalake")
    @patch("pipelines.datalake.extract_load.sisreg.tasks.handle_columns_to_bq")
    def test_dados_presentes_chama_upload(self, mock_handle, mock_upload) -> None:
        import prefect

        df = pd.DataFrame({"col": [1, 2]})
        mock_handle.run.return_value = df

        with patch("pipelines.utils.credential_injector.inject_bd_credentials", MagicMock()):
            with prefect.context(parameters={"environment": "staging"}, logger=MagicMock()):
                from pipelines.datalake.extract_load.sisreg.tasks import (
                    normalizar_e_subir,
                )

                resultado = normalizar_e_subir.run(
                    environment="staging",
                    conjunto="escalas",
                    dataset_id="brutos_sisreg_web",
                    tabelas_consolidadas={"escalas": df},
                )
        self.assertTrue(resultado)
        mock_upload.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
