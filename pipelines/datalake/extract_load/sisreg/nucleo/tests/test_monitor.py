# -*- coding: utf-8 -*-
"""
Testes do monitor de frescor (monitor.py): logica de SLA, alerta Discord
e configuracao do fluxo proprio.

Todos offline: BigQuery e Discord mockados.
"""
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

_FUSO_SP = ZoneInfo("America/Sao_Paulo")


class TestCalcularSlaViolacoes(unittest.TestCase):
    """Testa a logica de deteccao de violacoes de SLA."""

    def _df_log(self, registros: list) -> pd.DataFrame:
        return pd.DataFrame(registros, columns=["conjunto", "as_of", "status"])

    def test_sem_registros_e_violacao(self) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            _calcular_sla_violacoes,
        )

        df_vazio = self._df_log([])
        agora = datetime.now(_FUSO_SP)
        violacoes = _calcular_sla_violacoes(df_vazio, agora)
        # Todos os conjuntos estao sem registro = todos violam
        from pipelines.datalake.extract_load.sisreg import constants as C

        self.assertEqual(len(violacoes), len(C.SLA_FRESCOR_DIAS))

    def test_execucao_recente_nao_e_violacao(self) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            _calcular_sla_violacoes,
        )

        agora = datetime.now(_FUSO_SP)
        ontem = (agora - timedelta(hours=2)).isoformat()
        df = self._df_log([("escalas", ontem, "OK")])
        violacoes = _calcular_sla_violacoes(df, agora)
        conjuntos_violados = [v["conjunto"] for v in violacoes]
        self.assertNotIn("escalas", conjuntos_violados)

    def test_execucao_antiga_e_violacao(self) -> None:
        from pipelines.datalake.extract_load.sisreg import constants as C
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            _calcular_sla_violacoes,
        )

        sla_escalas = C.SLA_FRESCOR_DIAS["escalas"]
        agora = datetime.now(_FUSO_SP)
        antigo = (agora - timedelta(days=sla_escalas + 1)).isoformat()
        df = self._df_log([("escalas", antigo, "OK")])
        violacoes = _calcular_sla_violacoes(df, agora)
        conjuntos_violados = [v["conjunto"] for v in violacoes]
        self.assertIn("escalas", conjuntos_violados)

    def test_status_falha_nao_conta_para_sla(self) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            _calcular_sla_violacoes,
        )

        agora = datetime.now(_FUSO_SP)
        recente = (agora - timedelta(hours=1)).isoformat()
        # FALHA recente nao conta como sucesso
        df = self._df_log([("escalas", recente, "FALHA")])
        violacoes = _calcular_sla_violacoes(df, agora)
        conjuntos_violados = [v["conjunto"] for v in violacoes]
        self.assertIn("escalas", conjuntos_violados)


class TestAlertarFrescorDiscord(unittest.TestCase):
    @patch("pipelines.datalake.extract_load.sisreg.nucleo.monitor.send_message")
    def test_envia_mensagem_ao_discord(self, mock_send) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            alertar_frescor_discord,
        )

        violacoes = [
            {"conjunto": "escalas", "ultimo_sucesso": None, "sla_dias": 2, "horas_atraso": 24}
        ]
        alertar_frescor_discord(violacoes)
        mock_send.assert_called_once()
        kwargs = mock_send.call_args.kwargs
        self.assertEqual(kwargs["monitor_slug"], "data-ingestion")

    @patch("pipelines.datalake.extract_load.sisreg.nucleo.monitor.send_message")
    def test_falha_de_discord_nao_levanta_excecao(self, mock_send) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            alertar_frescor_discord,
        )

        mock_send.side_effect = Exception("discord down")
        violacoes = [
            {"conjunto": "escalas", "ultimo_sucesso": None, "sla_dias": 2, "horas_atraso": 1}
        ]
        # Nao deve levantar
        alertar_frescor_discord(violacoes)


class TestSisregMonitorFlow(unittest.TestCase):
    """Testa a configuracao do fluxo de monitoramento (independente da extracao)."""

    def test_fluxo_tem_schedule_configurado(self) -> None:
        from prefect.schedules import Schedule

        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            sisreg_monitor_flow,
        )

        self.assertIsNotNone(sisreg_monitor_flow.schedule)
        self.assertIsInstance(sisreg_monitor_flow.schedule, Schedule)

    def test_fluxo_contem_task_verificar_frescor(self) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            sisreg_monitor_flow,
        )

        nomes = {t.name for t in sisreg_monitor_flow.tasks}
        self.assertTrue(
            any("frescor" in n or "verificar" in n for n in nomes),
            msg=f"Task de frescor nao encontrada. Tasks presentes: {nomes}",
        )

    def test_fluxo_tem_parametros_environment_e_dataset_id(self) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            sisreg_monitor_flow,
        )

        param_names = {p.name for p in sisreg_monitor_flow.parameters()}
        self.assertIn("environment", param_names)
        self.assertIn("dataset_id", param_names)

    def test_nome_do_fluxo(self) -> None:
        from pipelines.datalake.extract_load.sisreg.nucleo.monitor import (
            sisreg_monitor_flow,
        )

        self.assertIn("Monitor", sisreg_monitor_flow.name)
        self.assertIn("SISREG", sisreg_monitor_flow.name)


if __name__ == "__main__":
    unittest.main()
