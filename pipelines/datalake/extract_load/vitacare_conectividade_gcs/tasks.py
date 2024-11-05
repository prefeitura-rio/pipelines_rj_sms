# -*- coding: utf-8 -*-
import json
import re

import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def handle_json_files_from_gcs(files):
    rows = []

    for file_metadata, file_content in files:
        row = dict()
        row["file_name"] = file_metadata.name
        row["gcs_created_at"] = file_metadata.time_created
        row["gcs_updated_at"] = file_metadata.updated
        row["datalake_loaded_at"] = pd.Timestamp.now().tz_localize("America/Sao_Paulo")

        cnes, date = file_metadata.name.split("-", 1)
        row["host_cnes"] = cnes

        naive_date = pd.to_datetime(date.replace(".json", ""), format="%Y-%m-%d-%Hh-%Mm")
        row["host_created_at"] = naive_date.tz_localize("America/Sao_Paulo")
        try:
            content_as_json = json.loads(file_content)
            row["content"] = json.dumps(content_as_json)
        except json.JSONDecodeError:
            row["content"] = str(file_content)

        rows.append(row)

    dataframe = pd.DataFrame.from_records(rows)

    return dataframe
