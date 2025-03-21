# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

import os
from time import sleep

from prefeitura_rio.pipelines_utils.logging import log
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

from pipelines.datalake.extract_load.sisreg_web.sisreg.utils import get_first_csv


class Sisreg:
    """
    A class representing the Sisreg system.

    Attributes:
        user (str): The username for logging in to the system.
        password (str): The password for logging in to the system.
        anti_captcha_key (str): The API key for the anti-captcha service.
        download_path (str): The path where the downloaded files will be saved.
        base64_image (str): The base64 encoded string representation of the captcha image.

    Methods:
        get_captcha_image: Retrieves the captcha image from a webpage.
        solve_captcha_and_login: Solves the captcha, enters the captcha text, and logs in to the system.
        download_escala: Downloads the escala from the Sisreg website.
    """

    def __init__(self, user, password, download_path):

        self._options = FirefoxOptions()
        self._options.add_argument("--headless")
        self._profile = FirefoxProfile()
        self._profile.set_preference("browser.download.folderList", 2)
        self._profile.set_preference("browser.download.manager.showWhenStarting", False)
        self._profile.set_preference("browser.download.dir", download_path)
        self._profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
        self._options.profile = self._profile

        self.browser = webdriver.Firefox(
            service=FirefoxService(GeckoDriverManager().install()),
            options=self._options,
        )
        self.browser.set_page_load_timeout(60)

        self.user = user
        self.password = password
        self.download_path = download_path
        self.base64_image = None

    def login(
        self,
        url="https://sisregiii.saude.gov.br/",
    ):
        """
        Logs into the Sisreg system using the provided username and password.

        Args:
            url (str, optional): The URL of the Sisreg login page. Defaults to "https://sisregiii.saude.gov.br/".

        Raises:
            PermissionError: If the login fails due to an incorrect username or password.

        Returns:
            None
        """

        self.browser.get(url)

        username_field = self.browser.find_element(By.NAME, "usuario")
        password_field = self.browser.find_element(By.NAME, "senha")

        username_field.send_keys(self.user)
        password_field.send_keys(self.password)

        entrar_button = self.browser.find_element(
            By.XPATH, "//input[@type='button'][@value='entrar']"
        )
        entrar_button.click()

        log("Entrar button clicked", level="debug")
        log(f"Current url: {self.browser.current_url}", level="debug")

        if self.browser.current_url == "https://sisregiii.saude.gov.br/cgi-bin/index":

            self.browser.switch_to.frame("f_main")
            log("Switched to frame f_main", level="debug")

            if "Leia com regularidade" in self.browser.page_source:
                log("Logged in successfully")
            elif "Senha expirada" in self.browser.page_source:
                log("Password expired. Please change your password.", level="error")
                self.browser.quit()
                raise PermissionError("Password expired. Please change your password.")
            else:
                log("Unknow login error", level="error")
                self.browser.quit()
                raise PermissionError("Unknown login error")
        else:
            log("Failed to log in", level="error")
            self.browser.quit()
            raise PermissionError("Failed to log in. Incorrect username or password")

    def download_escala(self):
        """
        Downloads the escala from the Sisreg website.

        Args:
            browser: The browser instance used to access the Sisreg website.

        Returns:
            None
        """
        log(f"Downloading Escala to {self.download_path}")

        try:
            self.browser.get(
                "https://sisregiii.saude.gov.br/cgi-bin/cons_escalas?radioFiltro=cpf&status=&dataInicial=&dataFinal=&qtd_itens_pag=50&pagina=&ibge=330455&ordenacao=&clas_lista=ASC&etapa=EXPORTAR_ESCALAS&coluna="
            )
        except Exception:  # pylint: disable=broad-except
            pass

        download_in_progress = True

        while download_in_progress:
            sleep(10)
            if any(file.endswith(".part") for file in os.listdir(self.download_path)):
                for file in os.listdir(self.download_path):

                    try:
                        if file.endswith(".part"):
                            file_size = os.path.getsize(file)
                            file_size_mb = file_size / (1024 * 1024)
                            log(
                                f"Current file size of {file} is {file_size_mb:.2f} MB.",
                                level="debug",
                            )
                    except FileNotFoundError:
                        log(f"File {file} not found in {self.download_path}", level="debug")

            else:
                download_in_progress = False
                log("Download finished!")

        return get_first_csv(self.download_path)
