# -*- coding: utf-8 -*-
"""
Testes do fluxo de monitoramento de frescor (monitor_flows.py).

Verifica que o fluxo importa corretamente, tem o schedule configurado
e os parametros esperados. Todos offline.
"""
import unittest


class TestSisregMonitorFlow(unittest.TestCase):
    """Testa a configuracao do fluxo de monitoramento."""

    def test_fluxo_importa_sem_erro(self) -> None:
        from pipelines.datalake.extract_load.sisreg.monitor_flows import (  # noqa: F401
            sisreg_monitor_flow,
        )

    def test_fluxo_tem_schedule_configurado(self) -> None:
        from prefect.schedules import Schedule

        from pipelines.datalake.extract_load.sisreg.monitor_flows import (
            sisreg_monitor_flow,
        )

        self.assertIsNotNone(sisreg_monitor_flow.schedule)
        self.assertIsInstance(sisreg_monitor_flow.schedule, Schedule)

    def test_fluxo_contem_task_verificar_frescor(self) -> None:
        from pipelines.datalake.extract_load.sisreg.monitor_flows import (
            sisreg_monitor_flow,
        )

        # Verifica que a task de frescor esta presente no fluxo
        nomes = {t.name for t in sisreg_monitor_flow.tasks}
        self.assertTrue(
            any("frescor" in n or "verificar" in n for n in nomes),
            msg=f"Task de frescor nao encontrada. Tasks presentes: {nomes}",
        )

    def test_fluxo_tem_parametros_environment_e_dataset_id(self) -> None:
        from pipelines.datalake.extract_load.sisreg.monitor_flows import (
            sisreg_monitor_flow,
        )

        param_names = {p.name for p in sisreg_monitor_flow.parameters()}
        self.assertIn("environment", param_names)
        self.assertIn("dataset_id", param_names)

    def test_nome_do_fluxo(self) -> None:
        from pipelines.datalake.extract_load.sisreg.monitor_flows import (
            sisreg_monitor_flow,
        )

        self.assertIn("Monitor", sisreg_monitor_flow.name)
        self.assertIn("SISREG", sisreg_monitor_flow.name)


if __name__ == "__main__":
    unittest.main()
