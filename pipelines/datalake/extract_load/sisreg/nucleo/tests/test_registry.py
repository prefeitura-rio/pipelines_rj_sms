# -*- coding: utf-8 -*-
"""
Testes do registry de conjuntos de dados (registry.py).

Verifica integridade estrutural: chaves unicas, perfis validos, tabelas
nao-vazias e funcoes reais dos extratores ligadas a cada conjunto.
"""
import dataclasses
import unittest

from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.nucleo.registry import (
    CONJUNTOS,
    ConjuntoSisreg,
    obter_conjunto,
)

_CHAVES_ESPERADAS = {"escalas", "afastamentos", "preparos", "solicitacoes", "fila_vagas"}
_PERFIS_VALIDOS = set(C.PERFIS_CREDENCIAL.keys())


class TestIntegridadeRegistry(unittest.TestCase):
    """Verifica que o registry foi construido corretamente."""

    def test_todas_as_chaves_presentes(self) -> None:
        self.assertEqual(set(CONJUNTOS.keys()), _CHAVES_ESPERADAS)

    def test_chave_do_conjunto_bate_com_a_chave_do_dict(self) -> None:
        for chave, conj in CONJUNTOS.items():
            with self.subTest(chave=chave):
                self.assertEqual(conj.chave, chave)

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

    def test_todos_conjuntos_apontam_para_funcoes_reais(self) -> None:
        """Cada conjunto deve estar ligado as funcoes do seu modulo extrator."""
        for chave, conj in CONJUNTOS.items():
            with self.subTest(chave=chave):
                self.assertTrue(callable(conj.planejar_trabalho))
                self.assertTrue(callable(conj.extrair_item))
                self.assertIn(
                    f"extractors.{chave}",
                    conj.extrair_item.__module__,
                    msg=f"{chave}.extrair_item nao vem do modulo extractors/{chave}.py",
                )


class TestObterConjunto(unittest.TestCase):
    def test_chave_valida_retorna_conjunto(self) -> None:
        conj = obter_conjunto("escalas")
        self.assertIsInstance(conj, ConjuntoSisreg)
        self.assertEqual(conj.chave, "escalas")

    def test_chave_invalida_levanta_key_error(self) -> None:
        with self.assertRaises(KeyError):
            obter_conjunto("conjunto_inexistente")


if __name__ == "__main__":
    unittest.main()
