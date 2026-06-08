# -*- coding: utf-8 -*-
"""
Testes da taxonomia de erros (errors.py).
"""
import unittest

from pipelines.datalake.extract_load.sisreg.errors import (
    ErroAutenticacao,
    ErroBloqueio,
    ErroEstrutura,
    ErroSisreg,
    ErroTransitorio,
    ErroUpload,
    ErroVazioSuspeito,
)


class TestErroSisreg(unittest.TestCase):
    """Testa construcao e formatacao do erro base."""

    def test_mensagem_simples(self) -> None:
        erro = ErroSisreg("algo quebrou")
        self.assertEqual(str(erro), "algo quebrou")

    def test_contexto_completo(self) -> None:
        erro = ErroSisreg(
            "falha",
            conjunto="escalas",
            etapa="extracao",
            item="item-1",
            url="https://example.com",
            detalhe="timeout",
        )
        texto = str(erro)
        self.assertIn("falha", texto)
        self.assertIn("conjunto=escalas", texto)
        self.assertIn("etapa=extracao", texto)
        self.assertIn("item=item-1", texto)
        self.assertIn("url=https://example.com", texto)
        self.assertIn("detalhe=timeout", texto)

    def test_contexto_parcial_omite_campos_vazios(self) -> None:
        erro = ErroSisreg("falha", conjunto="preparos")
        texto = str(erro)
        self.assertIn("conjunto=preparos", texto)
        self.assertNotIn("etapa=", texto)
        self.assertNotIn("item=", texto)

    def test_herda_de_exception(self) -> None:
        with self.assertRaises(Exception):
            raise ErroSisreg("erro")


class TestSubclasses(unittest.TestCase):
    """Testa hierarquia e captura seletiva das subclasses."""

    def test_todas_herdam_de_erro_sisreg(self) -> None:
        subclasses = [
            ErroAutenticacao,
            ErroBloqueio,
            ErroEstrutura,
            ErroVazioSuspeito,
            ErroUpload,
            ErroTransitorio,
        ]
        for cls in subclasses:
            with self.subTest(cls=cls.__name__):
                inst = cls("mensagem", conjunto="x")
                self.assertIsInstance(inst, ErroSisreg)
                self.assertEqual(inst.conjunto, "x")

    def test_captura_especifica_nao_captura_outras(self) -> None:
        """ErroBloqueio nao deve ser capturado por ErroAutenticacao."""
        capturado = False
        try:
            raise ErroBloqueio("bloqueado")
        except ErroAutenticacao:
            capturado = True
        except ErroBloqueio:
            # caminho esperado: ErroBloqueio nao e ErroAutenticacao
            pass
        self.assertFalse(capturado)

    def test_captura_ampla_via_base(self) -> None:
        """ErroSisreg captura qualquer subclasse."""
        for cls in [
            ErroAutenticacao,
            ErroBloqueio,
            ErroEstrutura,
            ErroVazioSuspeito,
            ErroUpload,
            ErroTransitorio,
        ]:
            with self.subTest(cls=cls.__name__):
                capturado = False
                try:
                    raise cls("x")
                except ErroSisreg:
                    capturado = True
                self.assertTrue(capturado)

    def test_campos_padrao_sao_strings_vazias(self) -> None:
        erro = ErroAutenticacao("falha")
        self.assertEqual(erro.conjunto, "")
        self.assertEqual(erro.etapa, "")
        self.assertEqual(erro.item, "")
        self.assertEqual(erro.url, "")
        self.assertEqual(erro.detalhe, "")


if __name__ == "__main__":
    unittest.main()
