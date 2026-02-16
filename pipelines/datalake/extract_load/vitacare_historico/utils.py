# -*- coding: utf-8 -*-

from datetime import datetime

import pandas as pd
import pytz
import requests
from google.auth.transport import requests as google_requests
from google.oauth2 import service_account

from pipelines.datalake.extract_load.vitacare_historico.constants import (
    vitacare_constants,
)
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log

# --- Funções auxiliares para pré-processamento ---


def clean_ut_id(val):
    """
    Decodifica e limpa valores VARBINARY de 'ut_id'
    """
    if isinstance(val, bytes):
        try:
            return val.decode("utf-16-le", errors="ignore").replace("\x00", "").strip()
        except UnicodeDecodeError:
            return val.decode("latin-1", errors="ignore").replace("\x00", "").strip()
    return str(val).replace("\x00", "").strip()


def transform_dataframe(df: pd.DataFrame, cnes_code: str, db_table: str) -> pd.DataFrame:
    """
    Aplica transformações DataFrames extraídos daq Vitacare
    """
    if df.empty:
        return df

    now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
    df["extracted_at"] = now
    df["id_cnes"] = cnes_code

    df.columns = remove_columns_accents(df)

    # Aplica clean_ut_id se a coluna existe
    if "ut_id" in df.columns:
        df["ut_id"] = df["ut_id"].apply(clean_ut_id)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.replace(r"[\n\r\t\x00]+", " ", regex=True)

    return df


def get_access_token(scopes: list = None) -> dict:
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    credentials = service_account.Credentials.from_service_account_file(
        vitacare_constants.SERVICE_ACCOUNT_FILE.value, scopes=scopes
    )
    credentials.refresh(google_requests.Request())
    token = credentials.token
    return {"Authorization": f"Bearer {token}"}


def get_instance_status() -> str:
    """
    Verifica o status atual da instância do Cloud SQL
    """
    project_id = vitacare_constants.PROJECT_ID.value
    instance_id = vitacare_constants.INSTANCE_ID.value
    url = (
        f"https://sqladmin.googleapis.com/sql/v1beta4/projects/{project_id}/instances/{instance_id}"
    )

    headers = get_access_token()
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return {
        "state": data.get("state"),
        "activationPolicy": data.get("settings", {}).get("activationPolicy"),
    }


def set_instance_activation_policy(policy: str):
    """
    Define a política de ativação da instância do Cloud SQL
    'ALWAYS' para ligar, 'NEVER' para desligar
    """
    project_id = vitacare_constants.PROJECT_ID.value
    instance_id = vitacare_constants.INSTANCE_ID.value
    url = (
        f"https://sqladmin.googleapis.com/sql/v1beta4/projects/{project_id}/instances/{instance_id}"
    )

    headers = get_access_token()
    data = {"settings": {"activationPolicy": policy}}

    response = requests.patch(url, headers=headers, json=data)
    response.raise_for_status()
    log(
        f"Política de ativação da instância '{instance_id}' definida para '{policy}'. Operação iniciada.",
        level="info",
    )
