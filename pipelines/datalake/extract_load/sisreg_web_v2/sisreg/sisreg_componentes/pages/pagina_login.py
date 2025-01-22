# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

from prefeitura_rio.pipelines_utils.logging import log
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException
)

from sisreg_componentes.pages.base_page import BasePage

class PaginaLogin(BasePage):
    """
    Página de Login no SISREG.
    """

    def fazer_login(self, usuario: str, senha: str) -> None:
        """
        Faz login no SISREG usando as credenciais fornecidas.

        Args:
            usuario (str): Nome de usuário para login.
            senha (str): Senha para login.
        """
        try:
            # Abre a tela inicial (página de login) e aguarda o botão de "entrar"
            self.abrir_pagina(
                url_complemento="",
                seletor_espera=(By.NAME, "entrar"),
                tempo_espera=30
            )

            # Preenche formulário
            self._preencher_formulario_login(usuario, senha)

            # Clica no botão "entrar"
            self._submeter_formulario()

            # Aguarda frame principal e muda para ele
            self.mudar_para_frame((By.NAME, "f_principal"), tempo_espera=30)

        except (TimeoutException, NoSuchElementException) as e:
            log(f"Erro durante o processo de login: {str(e)}")
            raise
        except WebDriverException as e:
            log(f"Ocorreu um erro com o WebDriver no login: {str(e)}")
            raise
        finally:
            # Retorna ao contexto padrão
            self.voltar_contexto_padrao()

    def _preencher_formulario_login(self, usuario: str, senha: str) -> None:
        """
        Localiza campos de usuário e senha e preenche com as credenciais.
        """
        try:
            campo_usuario = self.browser.find_element(By.ID, "usuario")
            campo_senha = self.browser.find_element(By.ID, "senha")
        except NoSuchElementException as e:
            msg = ("Não foi possível localizar os campos de usuário (ID='usuario') "
                   "e senha (ID='senha'). Verifique se houve mudança no site.")
            log(msg)
            raise NoSuchElementException(msg) from e

        campo_usuario.send_keys(usuario)
        campo_senha.send_keys(senha)

    def _submeter_formulario(self) -> None:
        """
        Clica no botão "entrar" para submeter o formulário.
        """
        try:
            botao_entrar = self.browser.find_element(By.NAME, "entrar")
            botao_entrar.click()
        except NoSuchElementException as e:
            msg = "Botão 'entrar' não encontrado. Verifique se o elemento NAME='entrar' existe."
            log(msg)
            raise NoSuchElementException(msg) from e
