# -*- coding: utf-8 -*-
import pandas as pd
from google.cloud import storage

from pipelines.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import load_files_from_gcs_bucket
from pipelines.utils.time import from_relative_date


@task
def get_data(bucket_name:str, 
             file_prefix:str, 
             file_suffix:str,
             environment:str):
    
    client = storage.Client()
    
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=file_prefix)
    
    files = [x for x in blobs if x.name.endswith(file_suffix)]

    log(f'Formato do blob: {files[0]}')
    log(f'Tamanho do blob:{len(files)}')
    
    def blob_to_dict(blob):
        return {
            "name": blob.name,
            "updated": blob.updated,
            "created": blob.time_created,
        }
    
    log(f'Formato de data: {files[0].time_created}')
    log(f'Tipo do formato de data: {type(files[0].time_created)}')
        
    file_metadata = [blob_to_dict(x) for x in files]
    
    return file_metadata

@task
def get_base_date(base_date):
    log("Getting target date")
    if base_date == "today" or not base_date:
        base_date = pd.Timestamp.now().strftime("%Y-%m-%d")
        log(f"Using today's date: {base_date}")
    elif base_date == "yesterday":
        base_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        log(f"Using yesterday's date: {base_date}")
    else:
        base_date = pd.Timestamp(base_date).strftime("%Y-%m-%d")
        log(f"Using specified date: {base_date}")

    return base_date