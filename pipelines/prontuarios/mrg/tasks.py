# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import Tuple

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.prontuarios.mrg.ranking import *


@task
def get_params(start_datetime: str) -> dict:
    """
    Creating params
    Args:
        start_datetime (str) : initial date extraction

    Returns:
        dict : params dictionary
    """
    end_datetime = start_datetime + timedelta(days=1)
    log(
        f"""
        Standardizing from {start_datetime.strftime("%Y-%m-%d 00:00:00")}
        to {end_datetime.strftime("%Y-%m-%d 00:00:00")}"""
    )
    return {
        "start_datetime": start_datetime.strftime("%Y-%m-%d 00:00:00"),
        "end_datetime": end_datetime.strftime("%Y-%m-%d 00:00:00"),
        "datasource_system": "smsrio",
    }

@task
def normalize_payload_list(register_list):
    df_patient = pd.DataFrame(register_list)
    df_patient_normalized = pd.concat([
                            df_patient[["patient_code","event_moment","ingestion_moment"]],
                            pd.json_normalize(df_patient['standardized_record']).drop('patient_code',axis=1),
                            pd.json_normalize(df_patient['source'])
                        ],axis=1
                    )
    return df_patient_normalized

@task
def first_merge(group):
    fields_to_pass = ['patient_code', 'event_moment', 'ingestion_moment', 'active', 'raw_source_id', 'is_valid', 'system', 'cnes', 'description']
    merged_payload = {}
    not_merged_payload = {}
    for field in [column for column in group.columns if column not in fields_to_pass]:
        field_samples = group.loc[:,[field,'system','event_moment']]
        field_samples_unique = field_samples.drop_duplicates(subset = [field])
        
        if len(field_samples_unique) == 1:
            merged_payload[field] = field_samples_unique[field].values[0]
        else:
            data_ranked = ranking_values(field_samples_unique,field)
            not_merged_payload[field] = data_ranked
            
    return merged_payload, not_merged_payload

@task
def final_merge(merged_payload,not_merged_payload):
    if len(not_merged_payload) == 0:
        return merged_payload
    else:
        merging_preferences = {
            'birth_city_cod':'replace',
            'birth_state_cod':'replace',
            'birth_country_cod':'replace',
            'deceased':'replace',
            'deceased_date':'replace',
            'father_name':'replace',
            'gender':'replace',
            'mother_name':'replace',
            'name':'replace',
            'nationality':'replace',
            'race':'replace',
            'cns_list':'append',
            'address_list':'append',
            'telecom_list':'append'
        }

        for key,values in not_merged_payload.items():
            merging_type = merging_preferences[key]

            if merging_type == 'append':
                unique_values = []
                for value in values.loc[:,key].values:
                    if value not in unique_values:
                        unique_values.append(value)
                merged_payload[key] = unique_values
            elif merging_type == 'replace':
                merged_payload[key] = values.loc[values['ranking']==1,key].values[0]
            else:
                raise('Invalid type of merge')
        return merged_payload

