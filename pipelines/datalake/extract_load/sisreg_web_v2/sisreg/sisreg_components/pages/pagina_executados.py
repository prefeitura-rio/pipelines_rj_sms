# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By

from pipelines.datalake.utils.tasks import gerar_faixas_de_data
from pipelines.utils.logger import log


class PaginaExecutados:
    """
    Página de Executados no SISREG.
    """

    def baixar_executados(self) -> None:  # , data_inicial: str, data_final: str) -> None:
        """
        Extrai dados de executados no SISREG para o intervalo de datas especificado.
        Salva o resultado em CSV.

        Args:
            data_inicial (str): Data inicial no formato 'dd/mm/yyyy'.
            data_final (str): Data final no formato 'dd/mm/yyyy'.
            nome_arquivo_saida (str): Nome do arquivo CSV de saída.
        """

        if not os.path.exists(self.caminho_download):
            os.makedirs(self.caminho_download, exist_ok=True)

        try:
            # TMP: Data Final é uma semana atras; data inicial é hoje
            data_inicial = datetime.now().strftime("%d/%m/%Y")
            data_final = (datetime.now() - timedelta(days=7)).strftime("%d/%m/%Y")
            # TMP: end

            lista_datas = gerar_faixas_de_data.run(data_inicial, data_final)
            if not lista_datas:
                log("Nenhuma data válida foi gerada. Encerrando extrair_executados.")
                return

            dataframes = []
            for data_str in lista_datas:
                df_dia = self._extrair_tabela_dia(data_str)
                if df_dia is not None and not df_dia.empty:
                    dataframes.append(df_dia)

            if not dataframes:
                log("Nenhum dado encontrado para o período especificado.")
                return

            df_final = pd.concat(dataframes, ignore_index=True)
            df_final.to_csv(
                os.path.join(self.caminho_download, "executados.csv"), index=False, encoding="utf-8"
            )
            log(f"Executados consolidados salvos em: {self.caminho_download}")

            return os.path.join(self.caminho_download, "executados.csv")

        except (TimeoutException, NoSuchElementException, WebDriverException) as e:
            log(f"Erro na extração de executados: {str(e)}")
            raise
        finally:
            self.voltar_contexto_padrao()

    def _extrair_tabela_dia(self, data_str: str) -> pd.DataFrame:
        """
        Abre a página de 'executados' para uma data específica e extrai a tabela.

        Args:
            data_str (str): Data no formato dd/mm/yyyy.

        Returns:
            pd.DataFrame: DataFrame com dados da tabela ou None se não encontrado.
        """
        url_executados = (
            "cgi-bin/gerenciador_solicitacao"
            "?etapa=LISTAR_SOLICITACOES&co_solicitacao=&cns_paciente=&no_usuario="
            "&cnes_solicitante=&cnes_executante=&co_proc_unificado=&co_pa_interno="
            "&ds_procedimento=&tipo_periodo=E"
            f"&dt_inicial={data_str}"
            f"&dt_final={data_str}"
            "&cmb_situacao=7&qtd_itens_pag=0&co_seq_solicitacao=&ordenacao=2&pagina=0"
        )

        # Abre a página com retries para lidar com instabilidade
        if not self._abrir_executados_com_retentativas(url_executados):
            return None

        try:
            return self._parse_tabela_listagem()
        except Exception as e:
            log(f"Erro ao extrair tabela para {data_str}: {e}")
            return None

    def _abrir_executados_com_retentativas(
        self, url_complemento: str, max_tentativas: int = 50
    ) -> bool:
        """
        Tenta abrir a página de executados repetidas vezes, caso falhe por timeouts ou instabilidades.

        Args:
            url_complemento (str): Complemento da URL relativa (sem a URL base).
            max_tentativas (int): Número máximo de tentativas.

        Returns:
            bool: True se abriu com sucesso, False caso contrário.
        """
        for tentativa in range(max_tentativas):
            try:
                self.abrir_pagina(
                    url_complemento=url_complemento,
                    seletor_espera=(By.NAME, "pesquisar"),
                    tempo_espera=300,
                )
                return True
            except Exception as e:
                log(f"Tentativa {tentativa+1}/{max_tentativas} falhou ao abrir executados: {e}")
                time.sleep(5)
        return False

    def _parse_tabela_listagem(self) -> pd.DataFrame:
        """
        Localiza a tabela de listagem e extrai seus dados em um DataFrame.
        """
        # A classe table_listagem aparece várias vezes; a segunda (index 1) costuma ter os dados.
        tabelas = self.browser.find_elements(By.CLASS_NAME, "table_listagem")
        if len(tabelas) < 2:
            return None

        soup = BeautifulSoup(tabelas[1].get_attribute("outerHTML"), "html.parser")
        tbody = soup.find("tbody")
        if not tbody:
            return None

        linhas = tbody.find_all("tr")
        dados = [
            [col.text.strip() for col in linha.find_all("td")]
            for linha in linhas
            if "RETORNADAS" not in linha.text
        ]

        if not dados:
            return None

        colunas = dados[0]
        dados = dados[1:]
        if not colunas or not dados:
            return None

        df = pd.DataFrame(dados, columns=colunas)
        return df
