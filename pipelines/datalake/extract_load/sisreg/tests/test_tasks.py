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
from pipelines.datalake.extract_load.sisreg.resultado import (
    Consolidado,
    ResultadoConjunto,
)


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

    def _resultado(self, tabelas: dict, total: int = 1, ok: int = 1) -> ResultadoConjunto:
        return ResultadoConjunto(tabelas=tabelas, total=total, ok=ok)

    def _rodar(self, conjunto, fragmento, data_extracao="2025-01-01"):
        """Executa consolidar.run() com os argumentos dados."""
        from pipelines.datalake.extract_load.sisreg.tasks import consolidar

        return consolidar.run(
            conjunto=conjunto,
            fragmento=fragmento,
            data_extracao=data_extracao,
        )

    def test_fragmento_ok_retorna_dados(self) -> None:
        # "solicitacoes" tem colunas_esperadas=frozenset() - sem validacao de schema
        df = pd.DataFrame({"col": [1, 2, 3, 4]})
        resultado = self._rodar("solicitacoes", self._resultado({"solicitacoes": df}))
        self.assertIsNotNone(resultado.tabelas)
        self.assertIn("solicitacoes", resultado.tabelas)
        self.assertEqual(len(resultado.tabelas["solicitacoes"]), 4)

    def test_fragmento_none_cancela_upload(self) -> None:
        """Gate de task: None (task failure) cancela o upload."""
        resultado = self._rodar("solicitacoes", None)
        self.assertIsNone(resultado.tabelas)

    def test_fragmento_com_tabelas_vazias_retorna_tabelas_none(self) -> None:
        """Fragmento com todos os DataFrames vazios resulta em tabelas=None."""
        r = self._resultado({"solicitacoes": pd.DataFrame()})
        resultado = self._rodar("solicitacoes", r)
        self.assertIsNone(resultado.tabelas)

    def test_coluna_particao_adicionada(self) -> None:
        from pipelines.datalake.extract_load.sisreg import constants as C

        df = pd.DataFrame({"col": [1]})
        resultado = self._rodar(
            "solicitacoes",
            self._resultado({"solicitacoes": df}),
            data_extracao="2025-06-01",
        )
        self.assertIn(C.COLUNA_PARTICAO, resultado.tabelas["solicitacoes"].columns)
        self.assertEqual(resultado.tabelas["solicitacoes"][C.COLUNA_PARTICAO].iloc[0], "2025-06-01")

    def test_schema_drift_levanta_erro_estrutura(self) -> None:
        """Colunas esperadas ausentes devem levantar ErroEstrutura."""
        from pipelines.datalake.extract_load.sisreg.errors import ErroEstrutura

        df = pd.DataFrame({"coluna_inesperada": [1, 2]})
        with self.assertRaises(ErroEstrutura):
            self._rodar("afastamentos", self._resultado({"afastamentos": df}))

    def test_tabela_sem_colunas_esperadas_nao_falha(self) -> None:
        """Tabelas com colunas_esperadas=frozenset() passam sem validacao de schema."""
        df = pd.DataFrame({"qualquer_coluna": [1, 2]})
        resultado = self._rodar("solicitacoes", self._resultado({"solicitacoes": df}))
        self.assertIsNotNone(resultado.tabelas)

    @patch("pipelines.utils.monitor.send_message", MagicMock())
    def test_sub_item_falho_cancela_upload(self) -> None:
        """Gate de sub-itens: ids_falhos cancela o upload."""
        df = pd.DataFrame({"col": [1]})
        r = ResultadoConjunto(tabelas={"solicitacoes": df}, total=10, ok=9, ids_falhos=["cpf_3"])
        resultado = self._rodar("solicitacoes", r)
        self.assertIsNone(resultado.tabelas)
        self.assertEqual(resultado.metricas["status"], "FALHA_PARCIAL")

    def test_metricas_status_ok_em_sucesso(self) -> None:
        df = pd.DataFrame({"col": [1]})
        r = self._resultado({"solicitacoes": df}, total=5, ok=5)
        resultado = self._rodar("solicitacoes", r)
        self.assertEqual(resultado.metricas["status"], "OK")
        self.assertEqual(resultado.metricas["items_total"], 5)
        self.assertEqual(resultado.metricas["items_ok"], 5)


class TestPlanejarTrabalho(unittest.TestCase):
    """Testa a logica de suspicious-empty."""

    def test_item_vazio_em_conjunto_nunca_vazio_levanta_erro(self) -> None:
        from pipelines.datalake.extract_load.sisreg.tasks import planejar_trabalho

        mock_conj = MagicMock()
        mock_conj.planejar_trabalho.return_value = {}

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

    def test_item_vazio_em_preparos_nao_levanta(self) -> None:
        """preparos pode retornar dict vazio sem suspeita."""
        from pipelines.datalake.extract_load.sisreg.tasks import planejar_trabalho

        mock_conj = MagicMock()
        mock_conj.planejar_trabalho.return_value = {}

        with patch(
            "pipelines.datalake.extract_load.sisreg.tasks.obter_conjunto",
            return_value=mock_conj,
        ):
            resultado = planejar_trabalho.run(
                conjunto="preparos",
                credenciais={},
                params={},
            )
            self.assertEqual(resultado, {})

    def test_item_nao_vazio_retorna_item(self) -> None:
        from pipelines.datalake.extract_load.sisreg.tasks import planejar_trabalho

        item = {"id": "todos_os_cpfs", "cpfs": ["111"]}
        mock_conj = MagicMock()
        mock_conj.planejar_trabalho.return_value = item

        with patch(
            "pipelines.datalake.extract_load.sisreg.tasks.obter_conjunto",
            return_value=mock_conj,
        ):
            resultado = planejar_trabalho.run(
                conjunto="afastamentos",
                credenciais={},
                params={},
            )
            self.assertEqual(resultado, item)


class TestNormalizarESubir(unittest.TestCase):
    """Testa a logica de normalizar_e_subir via a funcao interna (offline).

    authenticated_task exige contexto Prefect completo; testamos a logica de
    decisao (skip em Consolidado vazio, chamada a upload em dados presentes).
    """

    @patch("pipelines.datalake.extract_load.sisreg.tasks.upload_df_to_datalake")
    @patch("pipelines.datalake.extract_load.sisreg.tasks.handle_columns_to_bq")
    def test_consolidado_sem_tabelas_pula_upload(self, mock_handle, mock_upload) -> None:
        """Com consolidado.tabelas=None a funcao deve retornar False sem chamar upload."""
        import prefect

        df = pd.DataFrame({"col": [1]})
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
                    consolidado=Consolidado(tabelas=None, metricas={"status": "FALHA"}),
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
                    consolidado=Consolidado(tabelas={"escalas": df}, metricas={"status": "OK"}),
                )
        self.assertTrue(resultado)
        mock_upload.run.assert_called_once()

    @patch("pipelines.datalake.extract_load.sisreg.tasks.upload_df_to_datalake")
    @patch("pipelines.datalake.extract_load.sisreg.tasks.handle_columns_to_bq")
    def test_modo_escrita_por_conjunto(self, mock_handle, mock_upload) -> None:
        """escalas usa overwrite; preparos (paginado) usa append."""
        import prefect

        df = pd.DataFrame({"col": [1]})
        mock_handle.run.return_value = df

        with patch("pipelines.utils.credential_injector.inject_bd_credentials", MagicMock()):
            with prefect.context(parameters={"environment": "staging"}, logger=MagicMock()):
                from pipelines.datalake.extract_load.sisreg.tasks import (
                    normalizar_e_subir,
                )

                normalizar_e_subir.run(
                    environment="staging",
                    conjunto="escalas",
                    dataset_id="brutos_sisreg_web",
                    consolidado=Consolidado(tabelas={"escalas": df}, metricas={}),
                )
                self.assertEqual(mock_upload.run.call_args.kwargs["dump_mode"], "overwrite")

                mock_upload.reset_mock()
                normalizar_e_subir.run(
                    environment="staging",
                    conjunto="preparos",
                    dataset_id="brutos_sisreg_web",
                    consolidado=Consolidado(tabelas={"preparos": df}, metricas={}),
                )
                self.assertEqual(mock_upload.run.call_args.kwargs["dump_mode"], "append")


if __name__ == "__main__":
    unittest.main()
