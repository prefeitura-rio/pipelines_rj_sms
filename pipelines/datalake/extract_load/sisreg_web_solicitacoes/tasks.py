from datetime import datetime, timedelta
import requests
import sys
import time
import hashlib
from typing import Iterable
from bs4 import BeautifulSoup, SoupStrainer
from pathlib import Path
from typing import Dict, Iterable, Optional
from pipelines.datalake.extract_load.sisreg_web_solicitacoes.parser import parse_codigos_sisreg
from pipelines.datalake.extract_load.sisreg_web_solicitacoes.constants import *

from pipelines.utils.credential_injector import authenticated_task as task


# UTILS ------------------------------------------------------
def sha256_upper(txt: str) -> str:
    """Hash SHA-256 da senha MAIÚSCULA (mesma lógica do JS)."""
    return hashlib.sha256(txt.upper().encode("utf-8")).hexdigest()

def hidden(html: str) -> Dict[str, str]:
    """Extrai inputs hidden de um html para reaproveitar no post de login."""
    only_hidden = SoupStrainer("input")
    soup = BeautifulSoup(html, "lxml", parse_only=only_hidden)
    return {
        i["name"]: i.get("value", "")
        for i in soup.find_all("input", type="hidden")
        if i.get("name")
    }

def iter_codes(codigos: list[str]):
    """
    Iterador simples para percorrer a lista de códigos.
    """
    for codigo in codigos:
        yield codigo

def _looks_like_login_page(html: str) -> bool:
    # se o formulário de login ainda aparece, não logou
    return ('name="usuario"' in html) and ('name="senha"' in html)


# TAREFAS ------------------------------------------------------
# Tarefa 1 - Login no SISREG
def login(
        sess: requests.Session, user: str, pwd: str,
        max_retries: int = 3, base_backoff: float = 0.75
) -> None:

    for attempt in range(1, max_retries + 1):
        try:
            # vai até a página de login
            r = sess.get(LOGIN_PAGE, timeout=30)
            r.raise_for_status()

            # pega os campos hidden do formulário de login
            fields = hidden(r.text)

            # loga
            payload = {
                **fields,
                "usuario": user,
                "senha": "",
                "senha_256": sha256_upper(pwd),
                "etapa": "ACESSO",
                "logout": "",
            }
            r = sess.post(LOGIN_POST, data=payload, allow_redirects=True, timeout=30)
            r.raise_for_status()

            # valida login indo a pagina de perfil
            time.sleep(1)
            chk = sess.get(CONFIG_PERFIL_URL, allow_redirects=True, timeout=30)
            chk.raise_for_status()
            time.sleep(1)

            # checa se o login foi bem-sucedido
            # falhou se redirecionou de volta ao index (login) OU se o form de login apareceu
            if ("/cgi-bin/index" in chk.url) or _looks_like_login_page(chk.text):
                raise RuntimeError("Login falhou: não consegui acessar /cgi-bin/config_perfil.")

            print("Login realizado.")

            # ajusta o Referer para as próximas requisições
            sess.headers.update({"Referer": CONFIG_PERFIL_URL})

            return

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            RuntimeError
        ) as e:
            print(e)
            if attempt == max_retries:
                break
            
            # backoff exponencial
            sleep_s = base_backoff * (2 ** (attempt - 1))
            time.sleep(sleep_s)

    # levanta erro se não conseguiu logar após as tentativas
    raise RuntimeError(f"Falha no login após {max_retries} tentativas:")


# Tarefa 2 - Obter códigos das solicitações
def paginar_solicitacoes(
    sess: requests.Session,
    tipo_periodo: str,
    dt_inicial: str,
    dt_final: str,
    situacao: str,
    page_size: int,
    ordenacao: str,
    max_pages: Optional[int],
    wait: float,
) -> Iterable[Dict[str, str]]:
    """
    Itera sobre as todas páginas de solicitações do SISREG.
    Retorna um iterador com os dados de cada solicitação.
    Esta versão retorna apenas os códigos das solicitações.
    """

    # inicia variáveis para paginação
    page_index = 0
    got_total_pages = None
    total_rows = 0

    # lógica de paginação (repete os outros parametros)
    while True:

        params = {
            "etapa": "LISTAR_SOLICITACOES",
            "co_solicitacao": "",
            "cns_paciente": "",
            "no_usuario": "",
            "cnes_solicitante": "",
            "cnes_executante": "",
            "co_proc_unificado": "",
            "co_pa_interno": "",
            "ds_procedimento": "",
            "tipo_periodo": tipo_periodo,
            "dt_inicial": dt_inicial,
            "dt_final": dt_final,
            "cmb_situacao": situacao,
            "qtd_itens_pag": str(page_size),
            "co_seq_solicitacao": "",
            "ordenacao": ordenacao,
            "pagina": str(page_index),  # começa em 0
        }

        # carregando o html da page_index atual com retentativas
        for attempt in range(3):
            try:
                r = sess.get(GERENCIADOR_URL, params=params, timeout=360)
                r.raise_for_status()
                break

            except requests.RequestException as e:
                if attempt == 2:
                    raise
                print(
                    f"Erro ({attempt+1}/3) na página {page_index+1}: {e}. Retentando...",
                    file=sys.stderr,
                )
                time.sleep(wait * 3)

        # parseando o html retornado para pegar só os códigos das solicitações
        rows, total_pages, total_registros = parse_codigos_sisreg(r.text)

        # obtendo variavel de controle (got_total_pages) & printando na primeira passada
        if got_total_pages is None and total_pages is not None:
            got_total_pages = total_pages
            if total_registros is not None:
                print(
                    f"Detectados {total_registros} registros distribuídos em {total_pages} páginas."
                )

        # sai se não houver registros
        if not rows:
            print(f"Página {page_index+1}: 0 linhas. Encerrando.")
            return

        # yield dos registros encontrados
        for row in rows:
            yield row
        total_rows += len(rows)

        # log de progresso
        msg_total = f"/{got_total_pages}" if got_total_pages else ""
        print(
            f"Página {page_index+1}{msg_total}: {len(rows)} linhas (acumulado={total_rows})"
        )
        page_index += 1

        # verifica se chegou no limite de páginas
        if max_pages is not None and page_index >= max_pages:
            print("Chegou em max_pages, parando.")
            return

        if got_total_pages is not None and page_index >= got_total_pages:
            return

        # espera antes de continuar para a próxima página
        time.sleep(wait)


# Tarefa 3 - Salvar fichas do SISREG em HTML
# CADA FICHA CONTÉM TODAS AS INFORMAÇÕES PARA DETERMINADO CODIGO DE SOLICITAÇÃO DO SISREG
def salvar_fichas_html(
    sess,
    codigos: Iterable[str],
    wait: float,
) -> None:
    """
    Para cada código:
      1. Baixa a página VISUALIZAR_FICHA.
      2. Extrai somente o HTML dentro da classe 'table_listagem'.
      3. Salva em disco como <cod>_<data>.html dentro da pasta <data>_htmls.

    Observa as mesmas regras de headers/session já usadas no projeto.
    """
    # data atual no formato AAAAMMDD
    data_str = datetime.now().strftime("%Y%m%d")
 
    # nome da pasta onde serão gravados os arquivos html
    dir_path = Path(
        f"{data_str}_htmls"
    )
    dir_path.mkdir(parents=True, exist_ok=True)  # cria se não existir
 
    # para registrar códigos que falharem
    failed_codes = []
    print(f"Salvando fichas em: {dir_path}")
    for cod in iter_codes(codigos):
        # monta a url de get para a ficha
        # (aqui o cod é o código da solicitação)
 
        params = {
            "etapa": "VISUALIZAR_FICHA",
            "co_solicitacao": cod,
            "co_seq_solicitacao": cod,
            "ordenacao": "2",
            "pagina": "0",
        }
 
        # carrega a ficha com até 5 retentativas
        for attempt in range(5):
            try:
                r = sess.get(GERENCIADOR_URL, params=params, timeout=180)
                r.raise_for_status()
                break
 
            except requests.RequestException as e:
                print(f"Erro ({attempt+1}/5) ao baixar ficha {cod}: {e}")
                time.sleep(wait * 3)
        else:
            print(
                f"Falha ao baixar ficha {cod} após 5 tentativas. Encerrando o processo."
            )
            sys.exit(1)
 
        # extrai somente o trecho de interesse da página (com a classe 'table_listagem')
        soup = BeautifulSoup(r.text, "html.parser")
        tabela = soup.find(class_="table_listagem")
 
        # se não encontrou a tabela, avisa e encerra
        if not tabela:
            print(f"Aviso: não encontrei 'table_listagem' para {cod}.")
            failed_codes.append(cod)
 
        # define o caminho do arquivo de saída
        file_path = dir_path / f"{cod}_{data_str}.html"
 
        # salva o html contendo todos os dados de interesse da ficha
        with file_path.open("w", encoding="utf-8") as fp:
            fp.write(str(tabela))
 
    # log de sucesso
    print("Processamento completo das fichas.")
 
    # exibe os códigos que falharam ao salvar
    print("Códigos que falharam ao salvar:")
    print(failed_codes)

# --- 
@task(max_retries=5, retry_delay=timedelta(seconds=30))
def full_extract_process(usuario,
                         senha,
                         dt_inicial,
                         dt_final,
                         situacao,
                         tipo_periodo,
                         page_size,
                         ordenacao,
                         max_pages,
                         wait):
    
    # cria uma session para manter cookies e headers
    s = requests.Session()

    # cria um header padrão para a session
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Referer": LOGIN_PAGE,
        }
    )

    # faz o login no SISREG utilizando POST
    login(s, usuario, senha)

    # obtendo os códigos das solicitações
    rows_iter = paginar_solicitacoes(
        s,
        dt_inicial=dt_inicial,
        dt_final=dt_final,
        situacao=situacao,
        tipo_periodo=tipo_periodo,
        page_size=page_size,
        ordenacao=ordenacao,
        max_pages=max_pages,
        wait=wait,
    )

    # extrai apenas o código de cada linha
    # (usa stream para evitar carregar tudo na memória)
    codes_iter = (row["cod_solicitacao"] for row in rows_iter)

    # salva as fichas das solicitações em um arquivos
    salvar_fichas_html(
        s,
        codes_iter,
        wait=wait,
    )

    # fecha a session
    s.close()
