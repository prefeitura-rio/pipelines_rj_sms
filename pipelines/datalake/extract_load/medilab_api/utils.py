# -*- coding: utf-8 -*-
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry=retry_if_exception_type(Exception),
)
def get_token(api_url, usuario, senha, codacesso):
    response = requests.post(
        url=f"{api_url}/medisaudeapi/v1/getToken",
        json={"usuario": usuario, "senha": senha, "codacesso": codacesso},
    )
    response.raise_for_status()
    return response.json()["requestToken"]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry=retry_if_exception_type(Exception),
)
def get_study_list(api_url, token, dt_start, dt_end, patientcode):
    response = requests.post(
        url=f"{api_url}/medisaudeapi/v1/getStudyList",
        json={
            "requestToken": token,
            "dt_start": dt_start,
            "dt_end": dt_end,
            "patientcode": patientcode,
        },
    )
    response.raise_for_status()
    return response.json()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    retry=retry_if_exception_type(Exception),
)
def get_study_report(api_url, token, accession_number):
    response = requests.post(
        url=f"{api_url}/medisaudeapi/v1/getStudyReport",
        json={
            "requestToken": token,
            "accessionNumber": accession_number,
        },
    )
    response.raise_for_status()
    return response.json()
