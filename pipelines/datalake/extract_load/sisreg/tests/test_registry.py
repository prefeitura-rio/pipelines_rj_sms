# -*- coding: utf-8 -*-
"""
Testes do registry de conjuntos de dados (registry.py).

Verifica integridade estrutural: chaves unicas, perfis validos, tabelas
nao-vazias e callables registrados. Nenhum extrator real e importado -
os testes usam mocks para evitar dependencias em cascata.
"""
import dataclasses
import unittest
from unittest.mock import MagicMock

from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.registry import (
    CONJUNTOS,
    ConjuntoSisreg,
    listar_conjuntos,
    registrar_extrator,
)

_CHAVES_ESPERADAS = {"escalas", "afastamentos", "preparos", "solicitacoes", "fila_vagas"}
_PERFIS_VALIDOS = set(C.PERFIS_CREDENCIAL.keys())


class TestIntegridadeRegistry(unittest.TestCase):
    """Verifica que o registry foi construido corretamente."""

    def test_todas_as_chaves_presentes(self) -> None:
        self.assertEqual(set(CONJUNTOS.keys()), _CHAVES_ESPERADAS)

    def test_chaves_unicas(self) -> None:
        chaves = listar_conjuntos()
        self.assertEqual(len(chaves), len(set(chaves)))

    def test_perfis_de_credencial_sao_validos(self) -> None:
        for chave, conj in CONJUNTOS.items():
            with self.subTest(chave=chave):
                self.assertIn(
                    conj.perfil_credencial,
                    _PERFIS_VALIDOS,
                    msg=f"{chave}.perfil_credencial={conj.perfil_credencial!r} invalido",
                )

    def test_tabelas_nao_vazias(self) -> None:
        for chave, conj in CONJUNTOS.items():
            with self.subTest(chave=chave):
                self.assertTrue(
                    len(conj.tabelas) > 0,
                    msg=f"{chave} nao tem tabelas configuradas",
                )
                for tabela in conj.tabelas:
                    self.assertIsInstance(tabela, str)
                    self.assertTrue(tabela, msg=f"{chave}: nome de tabela vazio")

    def test_conjuntos_multi_tabela(self) -> None:
        """afastamentos e fila_vagas devem ter exatamente 2 tabelas."""
        self.assertEqual(len(CONJUNTOS["afastamentos"].tabelas), 2)
        self.assertEqual(len(CONJUNTOS["fila_vagas"].tabelas), 2)

    def test_conjuntos_single_tabela(self) -> None:
        """escalas, preparos e solicitacoes devem ter exatamente 1 tabela."""
        for chave in ("escalas", "preparos", "solicitacoes"):
            with self.subTest(chave=chave):
                self.assertEqual(len(CONJUNTOS[chave].tabelas), 1)

    def test_conjuntos_sao_imutaveis(self) -> None:
        conj = CONJUNTOS["escalas"]
        with self.assertRaises((dataclasses.FrozenInstanceError, AttributeError)):
            conj.chave = "outra_chave"  # type: ignore[misc]


class TestRegistrarExtrator(unittest.TestCase):
    """Verifica o mecanismo de registro de extratores."""

    def setUp(self) -> None:
        # Salva o estado original para restaurar apos cada teste
        self._backup = {k: v for k, v in CONJUNTOS.items()}

    def tearDown(self) -> None:
        # Restaura o estado original do registry
        CONJUNTOS.clear()
        CONJUNTOS.update(self._backup)

    def test_registrar_funcoes_substitui_callables(self) -> None:
        fn_planejar = MagicMock()
        fn_extrair = MagicMock()
        registrar_extrator("escalas", fn_planejar, fn_extrair)
        conj = CONJUNTOS["escalas"]
        self.assertIs(conj.planejar_trabalho, fn_planejar)
        self.assertIs(conj.extrair_item, fn_extrair)

    def test_registrar_chave_invalida_levanta_key_error(self) -> None:
        with self.assertRaises(KeyError):
            registrar_extrator("conjunto_inexistente", MagicMock(), MagicMock())

    def test_registro_nao_altera_outros_campos(self) -> None:
        original = CONJUNTOS["preparos"]
        registrar_extrator("preparos", MagicMock(), MagicMock())
        atualizado = CONJUNTOS["preparos"]
        self.assertEqual(atualizado.chave, original.chave)
        self.assertEqual(atualizado.perfil_credencial, original.perfil_credencial)
        self.assertEqual(atualizado.tabelas, original.tabelas)


class TestConjuntoSisregDataclass(unittest.TestCase):
    """Testa o comportamento basico do dataclass ConjuntoSisreg."""

    def test_instanciacao_minima(self) -> None:
        conj = ConjuntoSisreg(
            chave="teste",
            perfil_credencial="padrao",
            tabelas=("tb_teste",),
        )
        self.assertEqual(conj.chave, "teste")
        self.assertEqual(conj.tabelas, ("tb_teste",))

    def test_colunas_esperadas_padrao_e_vazio(self) -> None:
        conj = ConjuntoSisreg(
            chave="x",
            perfil_credencial="padrao",
            tabelas=("tb",),
        )
        self.assertEqual(conj.colunas_esperadas, {})


if __name__ == "__main__":
    unittest.main()
