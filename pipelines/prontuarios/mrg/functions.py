# -*- coding: utf-8 -*-
"""
Utils for merge flow
"""
from typing import Tuple
import pandas as pd


def ranking_values(df_unique, field, ranks_merge):
    from_to_dict = ranks_merge[ranks_merge['campo'] == field][[
        'smsrio', 'vitai']].to_dict(orient='records')[0]
    df_unique.loc[:, 'ranking'] = df_unique.loc[:, 'system'].map(from_to_dict)

    if len(df_unique[df_unique['ranking'] == 1]) != 1:
        sub_df_r1 = df_unique[df_unique['ranking'] == 1].copy()
        sub_df_resto = df_unique[df_unique['ranking'] != 1].copy()

        sub_df_r1.sort_values(by='event_moment', ascending=False, inplace=True)
        sub_df_r1['ranking'] = list(range(1, len(sub_df_r1) + 1))

        sub_df_resto.sort_values(by='ranking', ascending=False, inplace=True)
        sub_df_resto['ranking'] = range(len(sub_df_r1) + 1, len(df_unique) + 1)

        df_unique = pd.concat([sub_df_r1, sub_df_resto])

    return df_unique


def normalize_payload_list(register_list: dict) -> pd.DataFrame:
    data = register_list['mergeable_records']
    df_patient = pd.DataFrame(data)
    df_patient_normalized = pd.concat([
        df_patient[["event_moment", "ingestion_moment"]],
        pd.json_normalize(df_patient['standardized_record']),
        pd.json_normalize(df_patient['source'])
    ], axis=1
    )

    return df_patient_normalized


def first_merge(group: dict, ranking_df: pd.DataFrame) -> Tuple[dict, dict]:
    fields_to_pass = ['event_moment', 'ingestion_moment', 'active',
                      'raw_source_id', 'is_valid', 'system', 'cnes', 'description']
    merged_payload = {}
    not_merged_payload = {}
    for field in [column for column in group.columns if column not in fields_to_pass]:
        field_samples = group.loc[:, [field, 'system', 'event_moment']]
        field_samples_unique = field_samples.drop_duplicates(subset=[field])
        field_samples_unique = field_samples_unique.dropna()

        if len(field_samples_unique) == 0:
            merged_payload[field] = None
        elif len(field_samples_unique) == 1:
            merged_payload[field] = [c for (l, c) in field_samples_unique[field].items()][0]
        else:
            data_ranked = ranking_values(field_samples_unique, field, ranking_df)
            not_merged_payload[field] = data_ranked

    return merged_payload, not_merged_payload


def final_merge(merged_n_not_merged_payload: dict) -> dict:
    merged_payload = merged_n_not_merged_payload[0]
    not_merged_payload = merged_n_not_merged_payload[1]
    if len(not_merged_payload) == 0:
        return merged_payload
    else:
        merging_preferences = {
            'birth_city_cod': 'replace',
            'birth_state_cod': 'replace',
            'birth_country_cod': 'replace',
            'deceased': 'replace',
            'deceased_date': 'replace',
            'father_name': 'replace',
            'gender': 'replace',
            'mother_name': 'replace',
            'name': 'replace',
            'nationality': 'replace',
            'race': 'replace',
            'cns_list': 'append',
            'address_list': 'append',
            'telecom_list': 'append'
        }
        for key, values in not_merged_payload.items():
            merging_type = merging_preferences[key]

            if merging_type == 'append':
                unique_values = []
                for index, value in values.loc[:, key].items():
                    list_to_extend = clean_unique_values(unique_values, value)
                    unique_values.extend(list_to_extend)
                merged_payload[key] = unique_values
            elif merging_type == 'replace':
                merged_payload[key] = [
                    c for (l, c) in values.loc[values['ranking'] == 1, key].items()][0]
            else:
                raise ('Invalid type of merge')
        return merged_payload


def sanity_check(merged_payload: dict) -> dict:

    if merged_payload['cns_list'] is not None:
        cns_values = [cns['value'] for cns in merged_payload['cns_list']]
        if len(cns_values) != len(set(cns_values)):
            duplicated_cns = set([x for x in cns_values if cns_values.count(x) > 1])
            unique_cns_dics = merged_payload['cns_list'].copy()
            for cns_dic in merged_payload['cns_list']:
                if (cns_dic['value'] in duplicated_cns) & (cns_dic['is_main'] is True):
                    unique_cns_dics.remove(cns_dic)
            merged_payload['cns_list'] = unique_cns_dics

        main_cns = [cns for cns in merged_payload['cns_list'] if (cns['is_main'] is True)]
        if len(main_cns) > 1:
            for cns_dic in main_cns:
                merged_payload['cns_list'].remove(cns_dic)
                merged_payload['cns_list'].append({'value': cns_dic['value'], 'is_main': False})

    if (merged_payload['deceased_date'] is not None):
        if (~merged_payload['deceased']):
            merged_payload['deceased'] = True

    if merged_payload['address_list'] is not None:
        # Essa validação precisa ser feita em std e não foi
        for address in merged_payload['address_list']:
            if address['city'][0:2] != address['state']:
                address['state'] = address['city'][0:2]
            if address['country'] == '1':
                address['country'] = '010'

    return merged_payload


def load_ranking():
    return pd.read_csv('pipelines/prontuarios/mrg/merged_teste.csv')


def clean_unique_values(unique_list, new_value):
    if type(new_value) is list:
        return [value for value in new_value if value not in unique_list]
    else:
        if new_value in unique_list:
            return []
        else:
            return [new_value]
