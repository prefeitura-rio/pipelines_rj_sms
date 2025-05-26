# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

import os
import time

from prefeitura_rio.pipelines_utils.logging import log
from selenium.common.exceptions import TimeoutException, WebDriverException

from typing import List

import google
import google.api_core
from google.cloud import bigquery
from prefect.engine.signals import FAIL

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log

from bs4 import BeautifulSoup
import pandas as pd


class PaginaAfastamentos:
    """
    Página responsável por baixar dados de Afastamentos dos profissionais no SISREG.
    """

    def baixar_afastamentos(self) -> None:
        """
        Realiza o download dos afastamentos e salva os dados em um arquivo csv chamado 'afastamentos.csv'.

        Args:
            caminho_download (str): Caminho onde o arquivo CSV será baixado.
        """

        if not os.path.exists(self.caminho_download):
            os.makedirs(self.caminho_download, exist_ok=True)

        try:
            # Obtém os CPFs dos profissionais no BigQuery
            cpfs = self._get_cpfs_bq()
            dfs = []

            # Obtém os afastamentos para cada CPF
            total_cpfs = len(cpfs)
            for idx, cpf in enumerate(cpfs, 1):
                dfs.append(self._get_afastamentos_from_cpf(self.driver, cpf))

                progress = int((idx / total_cpfs) * 100)
                log(f"Progresso: {progress}% ({idx}/{total_cpfs})")

            # Concatena todos os DataFrames em um único
            df = pd.concat(dfs, ignore_index=True)  
            
            # Verifica se já existe um arquivo com este nome
            try:
                os.remove(f"afastamentos.csv")
            except FileNotFoundError:   
                pass

            # Escreve dados de afastamento no disco
            df.to_csv(os.path.join(self.caminho_download, "afastamentos.csv"), encoding="utf-8-sig", index=False)
            
            # Retorna o caminho absoluto do arquivo baixado
            return os.path.join(self.caminho_download, "afastamentos.csv")

        except WebDriverException as e:
            log(f"Erro ao acessar a página de oferta programada: {e}")
            raise

        finally:
            self.voltar_contexto_padrao()

    def _get_cpfs_bq_afastamentos(self) -> List[str]:
        """
        Obtém uma lista de CPFs dos profissionais a partir do BigQuery.

        Returns:
            List[str]: Lista de CPFs.
        """
        log("Obtendo CPFs dos profissionais no BigQuery...")

        bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

        query = """
        SELECT distinct profissional_executante_cpf
        FROM `rj-sms.saude_sisreg.oferta_programada_serie_historica`
        LIMIT 10
        """

        query_job = bq_client.query(query)
        cpfs = [row["profissional_executante_cpf"] for row in query_job]

        log(f"{len(cpfs)} CPFs encontrados.")

        return cpfs
    

    def _get_afastamentos_from_cpf(driver, cpf):
        log(f"Obtendo afastamentos para o CPF: {cpf}")

        # Acessa afastamentos do profissional
        driver.get(f"https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?cpf={cpf}&op=Log") 

        # Fecha os possíveis alertas que surgirem na pagina (ex: profissional nao encontrado)
        try:
            driver.switch_to.alert.accept() 
        except: 
            pass

        # Checa se página carregou
        if "CADASTRO DE AFASTAMENTO DE PROFISSIONAIS" in driver.page_source.upper(): 
            try:
                # Joga o conteudo da pag pro beautiful soup
                soup = BeautifulSoup(driver.page_source)
                
                # Encontra a tabela com os dados
                tables = soup.find_all('table', {'class': 'table_listagem'}) 

                all_data = []
                for table in tables:
                    # Pega linhas da tabela com exceção das duas primeiras (titulo da tabela e nomes das colunas)
                    rows = table.find_all('tr')[2:] 
                    
                    for row in rows:
                        # Pega todas as colunas da tabela
                        cols = row.find_all('td') 

                        # Pega o conteudo de cada celula, e da um tratamento especial pra coluna 3 (referente a "acoes")
                        data = [col.text.upper() if i != 3 else str(col) for i, col in enumerate(cols)] 
                        all_data.append(data)

                # Cria o dataframe definindo os nomes das colunas
                df = pd.DataFrame(all_data, columns = ["codigo", "data", "operador", "acao"])
                
                # Adiciona uma coluna com o cpf buscado
                df["cpf"] = cpf

                log(f"Afastamentos obtidos para o CPF: {cpf} com sucesso.")

                return df
            
            except Exception as e:
                log(f"Erro ao processar os dados do CPF {cpf}: {e}")
                raise
