# -*- coding: utf-8 -*-
import datetime
import re
import urllib.parse
from datetime import timedelta
from typing import List, Optional

import pandas as pd
import pytz

from pipelines.datalake.extract_load.tribunal_de_contas_rj.utils import (
    send_post_request,
    split_process_number
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def fetch_process_page(process_num: str, env: Optional[str]) -> List[str]:

    log(f"Fetching '{process_num}'")

    (sec, num, year) = split_process_number(process_num)

    if sec is None or num is None or year is None:
        raise RuntimeError(f"Error getting process number: {sec}/{num}/{year}")

    # Precisamos mandar um POST para esse URL
    # Enviando FormData: Sec=...&Num=...&Ano=...&TipoConsulta=PorNumero
    # Ex.: Sec=040&Num=101331&Ano=2021&TipoConsulta=PorNumero
    # Isso redireciona pra um 'GET /processo/Ficha?Ctid=1911377', onde Ctid é o ID interno
    URL = "https://etcm.tcmrio.tc.br/processo/Lista"
    html = send_post_request(URL, {
        "Sec": sec,
        "Num": num,
        "Ano": year,
        "TipoConsulta": "PorNumero"
    })
    # TODO: o site parece dar muito timeout; configurar uns retries que não
    #       dependam da task inteira falhar e retentar

    # Alguns processos estão vinculados a várias "referências", então abrem um
    # resultado de busca ao invés da página do processo diretamente
    # Ex.: 009/002324/2019, 040/100420/2019 (126)
    # Como detectar isso?
    # : <title>TCMRio - Portal do TCMRio  - Resultado de Pesquisa a Processos</title>
    # : ou talvez <h5>Foram encontrados {xxx} processos.</h5>
    # Meu entendimento é que só importa o processo em si, e não as referências
    # Acho que podemos pegar o primeiro 'a[href^="/processo/Ficha?Ctid="]' da página
    # Aí precisa fazer um GET manual a ele

    return html


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def scrape_decision_from_page(process_num: str, env: Optional[str]) -> List[str]:
    pass
    # A gente provavelmente quer pegar a tabela logo após "<h5>Decisões do Processo</h5>"
    # O formato é:
    # <div class="row">
    #   <div class="col-md-12">
    #     <h5>Decisões do Processo</h5>
    #   </div>
    # </div>
    # <div class="row">
    #   <div class="col-md-12 form-group">
    #     <table class="table table-striped ...">
    #       <thead> ... </thead>
    #       <tbody>
    #         <tr>
    #           <td>dd/mm/yyyy</td>
    #           <td>(texto que queremos)</td>

    # Alguns processo também têm 'processos apensos'
    # Ex: 009/001493/2020
    # É similar, mas uma tabela que aparece depois de um de "<h5>Processos Apensos</h5>"
    # ...
    # <tr>
    #   <td>
    #     <a href="/processo/Ficha?ctid=1831051">009/004365/2016</a>
    # E acredito que teria que pegar a última decisão de cada um desses apensos
