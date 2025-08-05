# -*- coding: utf-8 -*-
import base64
from datetime import datetime, timedelta

import pytz
from google.cloud import bigquery, storage

from pipelines.datalake.extract_load.medilab_api.utils import (
    get_study_list,
    get_study_report,
    get_token,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_exams_list_and_results(
    api_url,
    api_usuario,
    api_senha,
    api_codacesso,
    dt_start,
    dt_end,
    patientcode,
    output_dir,
    bucket_name,
):

    gcs_bucket_name = bucket_name
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)

    token = get_token(api_url, api_usuario, api_senha, api_codacesso)

    study_list_data = get_study_list(api_url, token, dt_start, dt_end, patientcode)

    if "studies" not in study_list_data or not study_list_data["studies"]:
        log(f"No 'studies' found or list is empty for patient {patientcode}", level="warning")
        return

    for study in study_list_data["studies"]:
        if "accessionNumber" not in study or not study["accessionNumber"]:
            log(f"Study for patient {patientcode} missing 'accessionNumber'", level="warning")
            continue

        current_accession_number = study["accessionNumber"]

        report_data = get_study_report(api_url, token, current_accession_number)

        if "arquivo" not in report_data or not report_data["arquivo"]:
            log(f"No 'arquivo' (base64 data) found for patient {patientcode}", level="warning")
            continue

        arquivo_b64 = report_data["arquivo"]

        study_date = datetime.now(pytz.timezone("America/Sao_Paulo"))
        study_date_str = study_date.strftime("%Y-%m-%d")

        filename = f"laudo_{patientcode}_{current_accession_number}.pdf"
        blob_path = f"{study_date_str}/{filename}"

        blob = bucket.blob(blob_path)

        if blob.exists():
            log(f"Arquivo {blob_path} já existe no bucket, pulando upload.", level="info")
        else:
            log(f"Uploading laudo to GCS as {blob_path}")

            blob.upload_from_string(base64.b64decode(arquivo_b64), content_type="application/pdf")

            log(f"Laudo uploaded successfully: gs://{gcs_bucket_name}/{blob_path}")


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_patient_code_from_bigquery(date_filter: str) -> list:
    query = f"""
       SELECT DISTINCT paciente_cpf
        FROM `rj-sms.brutos_sisreg_api.marcacoes`
        WHERE unidade_executante_id IN ('2970627')
        AND DATE(TIMESTAMP(data_marcacao)) = '{date_filter}'
        AND marcacao_executada = '1'
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
