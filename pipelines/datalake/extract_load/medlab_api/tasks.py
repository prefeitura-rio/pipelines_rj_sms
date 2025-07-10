# -*- coding: utf-8 -*-
import base64
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def get_exams_list_and_results(
    api_url, api_usuario, api_senha, api_codacesso, dt_start, dt_end, patientcode, output_dir
):

    result = requests.post(
        url=f"{api_url}/medisaudeapi/v1/getToken",
        json={"usuario": api_usuario, "senha": api_senha, "codacesso": api_codacesso},
    )
    result.raise_for_status()

    token = result.json()["requestToken"]

    result = requests.post(
        url=f"{api_url}/medisaudeapi/v1/getStudyList",
        json={
            "requestToken": token,
            "dt_start": dt_start,
            "dt_end": dt_end,
            "patientcode": patientcode,
        },
    )

    result.raise_for_status()

    study_list_data = result.json()

    if "studies" not in study_list_data or not study_list_data["studies"]:
        log(f"No 'studies' found or list is empty for patient {patientcode}", level="warning")
        return

    for study in study_list_data["studies"]:
        if "accessionNumber" not in study or not study["accessionNumber"]:
            log(f"Study for patient {patientcode} missing 'accessionNumber'", level="warning")
            continue

        current_accession_number = study["accessionNumber"]

        report_result = requests.post(
            url=f"{api_url}/medisaudeapi/v1/getStudyReport",
            json={
                "requestToken": token,
                "accessionNumber": current_accession_number,
            },
        )
        report_result.raise_for_status()

        report_data = report_result.json()

        if "arquivo" not in report_data or not report_data["arquivo"]:
            log(f"No 'arquivo' (base64 data) found for patient {patientcode}", level="warning")
            continue

        arquivo_b64 = report_data["arquivo"]

        os.makedirs(output_dir, exist_ok=True)

        filename = f"{patientcode}_{current_accession_number}.pdf"
        file_path = os.path.join(output_dir, filename)

        with open(file_path, "wb") as f:
            f.write(base64.b64decode(arquivo_b64))


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_patient_code_from_bigquery() -> list:
    query = """
       SELECT DISTINCT paciente_cpf
        FROM `rj-sms.brutos_sisreg_api.executados`
        WHERE unidade_executante_id IN ('2970627')
        AND PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*SZ", elastic__timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    """

    log(f"Fetching patient code: {query}")

    try:
        client = bigquery.Client()
        query_job = client.query(query)
        cpf_list = [row.paciente_cpf for row in query_job if row.paciente_cpf is not None]

        if not cpf_list:
            log("No patient CPFs found in BigQuery", level="warning")
            return []

        log(f"Found {len(cpf_list)} unique patient CPFs in BigQuery.")
        return cpf_list

    except Exception as e:
        log(f"Error fetching patient CPFs from BigQuery: {e}", level="error")
        raise
