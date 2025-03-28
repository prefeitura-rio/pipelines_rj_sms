# -*- coding: utf-8 -*-
from datetime import timedelta

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import load_file_from_bigquery


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def get_ap_from_cnes(cnes: str) -> str:

    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms", dataset_name="saude_dados_mestres", table_name="estabelecimento"
    )

    unidade = dados_mestres[dados_mestres["id_cnes"] == cnes]

    if unidade.empty:
        raise KeyError(f"CNES {cnes} not found in the database")

    ap = unidade.iloc[0]["area_programatica"]

    return f"AP{ap}"


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def get_healthcenter_name_from_cnes(cnes: str) -> str:

    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms", dataset_name="saude_dados_mestres", table_name="estabelecimento"
    )

    unidade = dados_mestres[dados_mestres["id_cnes"] == cnes]

    if unidade.empty:
        raise KeyError(f"CNES {cnes} not found in the database")

    nome_limpo = unidade.iloc[0]["nome_limpo"]
    ap = unidade.iloc[0]["area_programatica"]

    return f"(AP{ap}) {nome_limpo}"
