# -*- coding: utf-8 -*-
import json
from datetime import timedelta

import pandas as pd
import prefect
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from pipelines.tools.vitacare_healthcheck.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.googleutils import generate_bigquery_schema


@task()
def get_files_from_folder(folder_id):
    log("Authenticating with Google Drive")
    gauth = GoogleAuth(
        settings={
            "client_config_backend": "service",
            "service_config": {
                "client_json_file_path": "/tmp/credentials.json",
            },
        }
    )
    gauth.ServiceAuth()

    drive = GoogleDrive(gauth)

    log("Querying root folder")
    ap_folders = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()

    files = []
    for ap_folder in ap_folders:
        log(f"Querying for files in /{ap_folder}/ folder")
        ap_files = drive.ListFile(
            {"q": f"'{ap_folder['id']}' in parents and trashed=false"}
        ).GetList()

        for _file in ap_files:
            _file["ap"] = ap_folder["title"]
            files.append(_file)
    log("Finishing")

    return files


@task()
def get_structured_files_metadata(file_list):
    structured_file_list = []
    for _file in file_list:
        title = _file["title"].replace(" - Copia", "")
        datetime_str = title.split("-", 1)[1].split(".")[0]
        moment = pd.to_datetime(datetime_str, format="%Y-%m-%d-%Hh-%Mm")

        structured_file_list.append(
            {
                "title": title,
                "file_id": _file["id"],
                "file_fulltitle": _file["title"],
                "last_modified": _file["modifiedDate"],
                "ap": _file["ap"],
                "cnes": title.split("-")[0],
                "moment": moment,
            }
        )
    return structured_file_list


@task()
def filter_files_by_date(files, min_date, day_interval=1):
    max_date = min_date + pd.Timedelta(days=day_interval)

    min_date = min_date.strftime("%Y-%m-%d")
    max_date = max_date.strftime("%Y-%m-%d")

    structured_file_list = pd.DataFrame(files)
    data = structured_file_list[
        (structured_file_list["last_modified"] >= min_date)
        & (structured_file_list["last_modified"] < max_date)
    ]

    return data.to_dict(orient="records")


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_file_content(file_metadata):
    gauth = GoogleAuth(
        settings={
            "client_config_backend": "service",
            "service_config": {
                "client_json_file_path": "/tmp/credentials.json",
            },
        }
    )
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)

    file_id = file_metadata["file_id"]
    file = drive.CreateFile({"id": file_id})

    content = file.GetContentString(encoding="latin1")

    if len(content) == 0:
        file_content_json = {}
    else:
        try:
            file_content_json = json.loads(content)
        except json.JSONDecodeError:
            file_content_json = {}

    content_flattened = pd.json_normalize(file_content_json, sep="_")
    content_flattened["file_id"] = file_id
    content_flattened["cnes"] = file_metadata["cnes"]
    content_flattened["file_title"] = file_metadata["file_fulltitle"]
    content_flattened["ap"] = file_metadata["ap"]
    content_flattened["moment"] = file_metadata["moment"]
    content_flattened["file_last_modified"] = file_metadata["last_modified"]

    return content_flattened.to_dict(orient="records")[0]


@task(trigger=prefect.triggers.any_successful)
def json_records_to_dataframe(json_records):
    # Removing failed files
    json_records = [x for x in json_records if isinstance(x, dict)]

    dataframe = pd.DataFrame(json_records)
    dataframe.sort_values(by="moment", inplace=True)
    return dataframe


@task()
def fix_column_typing(dataframe):
    dataframe["interface_isVpn"] = dataframe["interface_isVpn"].astype(bool)
    return dataframe


@task()
def loading_data_to_bigquery(data, environment):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    project_name = constants.PROJECT_NAME.value[environment]
    dataset_name = constants.DATASET_NAME.value
    table_name = constants.TABLE_NAME.value

    bq_client.create_dataset(f"{project_name}.{dataset_name}", exists_ok=True)
    bq_client.create_table(f"{project_name}.{dataset_name}.{table_name}", exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=generate_bigquery_schema(data),
        write_disposition="WRITE_TRUNCATE",
    )
    job = bq_client.load_table_from_dataframe(
        data, f"{project_name}.{dataset_name}.{table_name}", job_config=job_config
    )
    job.result()
