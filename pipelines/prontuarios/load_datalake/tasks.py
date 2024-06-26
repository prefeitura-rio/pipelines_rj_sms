
from pipelines.utils.credential_injector import authenticated_task as task
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
import pandas as pd


@task
def load_file_from_bigquery(
    project_name: str,
    dataset_name: str,
    table_name: str,
    environment="dev",
):
    """
    Load data from BigQuery table into a pandas DataFrame.

    Args:
        project_name (str): The name of the BigQuery project.
        dataset_name (str): The name of the BigQuery dataset.
        table_name (str): The name of the BigQuery table.

    Returns:
        pandas.DataFrame: The loaded data from the BigQuery table.
    """
    client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference(project_name, dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)

    df = client.list_rows(table).to_dataframe()
    df_sample = df.sample(1000)
    df_sample.to_csv('sample.csv')

    log(f'Dataset loaded with {len(df)} rows')

    return df


@task
def clean_null_values(df: pd.DataFrame):
    """
    Delete null values from payload and prepare data to post
    """
    def normalize_list(row):
        new_row = []
        for dict in row:
            req_field = [i for i in dict.keys() if i in ['id_cbo', 'id_registro_conselho']][0]
            if dict[req_field] is None:
                pass
            else:
                new_row.append(dict)
        return new_row

    df['cbo'] = df['cbo'].apply(normalize_list)
    df['conselho'] = df['conselho'].apply(normalize_list)
    df['data_referencia'] = df['data_referencia'].astype(str)

    payload = df[['id_profissional_sus', 'cpf', 'cns', 'nome', 'cbo',
                  'conselho', 'data_referencia']].to_dict(orient='records')

    return payload
