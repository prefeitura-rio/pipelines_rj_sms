import requests
from bs4 import BeautifulSoup
import uuid
import pytz
import pandas as pd
from datetime import timedelta, datetime
from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def authenticate_and_fetch(
  username: str,
  password: str,
  apccodigo: str,
  dt_inicio: str = "2025-01-21T10:00:00-0300",
  dt_fim: str = "2025-01-21T11:30:00-0300",
):
  res = requests.get(
      url="https://cielab.lisnet.com.br/lisnetws/tokenlisnet/apccodigo",
      headers={
          'Content-Type': 'application/json',
          'apccodigo': apccodigo,
          'emissor': username,
          'pass': password
      }
  )
  res.raise_for_status()
  result = res.json()

  if res.get('status') != 200:
    raise Exception(result.get('mensagem'))

  token = result.get('token')

  res = requests.get(
      url="https://cielab.lisnet.com.br/lisnetws/APOIO/DTL/resultado",
      headers={
          'Content-Type': 'application/xml',
          'codigo': apccodigo,
          'token': token
      },
      data=f"""
      <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
      <lote>
          <codigoLis>1</codigoLis>
          <identificadorLis>1021</identificadorLis>
          <origemLis>1</origemLis>
          <dataResultado>
              <inicial>{dt_inicio}</inicial>
              <final>{dt_fim}</final>
          </dataResultado>
          <parametros>
              <parcial>N</parcial>
              <retorno>ESTRUTURADO</retorno>
          </parametros>
      </lote>
      """
  )
  res.raise_for_status()

  return res.text

@task(nout=3)
def transform(resultado_xml: str):
  soup = BeautifulSoup(resultado_xml, 'xml')

  solicitacoes_rows = []
  exames_rows = []
  resultados_rows = []

  lote = soup.find('lote')

  for solicitacao in lote.find_all('solicitacao'):
    solicitacao_id = str(uuid.uuid4())
    solicitacoes_row = solicitacao.attrs
    solicitacoes_row['id'] = solicitacao_id

    for entidade_suporte in ['responsaveltecnico','paciente']:
      for key, value in solicitacao.find(entidade_suporte).attrs.items():
        solicitacoes_row[f'{entidade_suporte}_{key}'] = value

    lote_attrs = lote.attrs
    for key, value in lote_attrs.items():
      solicitacoes_row[f'lote_{key}'] = value

    solicitacoes_rows.append(solicitacoes_row)

    for exame in solicitacao.find_all('exame'):
      exame_id = str(uuid.uuid4())
      exames_row = exame.attrs
      exames_row['id'] = exame_id
      exames_row['solicitacao_id'] = solicitacao_id

      for entidade_suporte in ['solicitante']:
        for key, value in solicitacao.find(entidade_suporte).attrs.items():
          exames_row[f'{entidade_suporte}_{key}'] = value

      exames_rows.append(exames_row)

      for resultado in exame.find_all('resultado'):
        resultado_row = resultado.attrs
        resultado_row['exame_id'] = exame_id

        resultados_rows.append(resultado_row)

  resultados_df = pd.DataFrame(resultados_rows)
  resultados_df['datalake_loaded_at'] = datetime.now(tz=pytz.timezone('America/Sao_Paulo'))

  exames_df = pd.DataFrame(exames_rows)
  exames_df['datalake_loaded_at'] = datetime.now(tz=pytz.timezone('America/Sao_Paulo'))

  solicitacoes_df = pd.DataFrame(solicitacoes_rows)
  solicitacoes_df['datalake_loaded_at'] = datetime.now(tz=pytz.timezone('America/Sao_Paulo'))

  return solicitacoes_df, exames_df, resultados_df