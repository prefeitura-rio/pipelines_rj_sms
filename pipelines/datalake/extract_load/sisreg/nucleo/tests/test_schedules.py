# -*- coding: utf-8 -*-
"""
Testes do agendamento (schedules.py).

Verifica que o schedule contem os clocks corretos e que os parameter_defaults
de cada clock incluem o conjunto e o dataset corretos.
"""
import unittest

from pipelines.datalake.extract_load.sisreg import constants as C


class TestSchedule(unittest.TestCase):
    def setUp(self) -> None:
        from pipelines.datalake.extract_load.sisreg.schedules import schedule

        self.schedule = schedule
        self.clocks = schedule.clocks

    def test_cinco_clocks_registrados(self) -> None:
        self.assertEqual(len(self.clocks), 5)

    def test_todos_os_conjuntos_estao_presentes(self) -> None:
        conjuntos = {c.parameter_defaults.get("conjunto") for c in self.clocks}
        esperados = {"escalas", "solicitacoes", "preparos", "afastamentos", "fila_vagas"}
        self.assertEqual(conjuntos, esperados)

    def test_todos_os_clocks_tem_environment_prod(self) -> None:
        for clock in self.clocks:
            with self.subTest(conjunto=clock.parameter_defaults.get("conjunto")):
                self.assertEqual(clock.parameter_defaults.get("environment"), "prod")

    def test_todos_os_clocks_tem_dataset_correto(self) -> None:
        for clock in self.clocks:
            with self.subTest(conjunto=clock.parameter_defaults.get("conjunto")):
                self.assertEqual(clock.parameter_defaults.get("dataset_id"), C.DATASET_ID_PADRAO)

    def test_preparos_tem_intervalo_semanal(self) -> None:
        from datetime import timedelta

        clock_preparos = next(
            c for c in self.clocks if c.parameter_defaults.get("conjunto") == "preparos"
        )
        self.assertEqual(clock_preparos.interval, timedelta(days=7))

    def test_escalas_tem_intervalo_diario(self) -> None:
        from datetime import timedelta

        clock_escalas = next(
            c for c in self.clocks if c.parameter_defaults.get("conjunto") == "escalas"
        )
        self.assertEqual(clock_escalas.interval, timedelta(days=1))


if __name__ == "__main__":
    unittest.main()
