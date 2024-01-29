from io import StringIO
import os
from google.cloud import storage
import pandas as pd
from pipelines.utils.infisical import inject_bd_credentials
from pipelines.prontuarios.raw.smsrio.tasks import (
    transform_merge_patient_and_cns_data,
    transform_data_to_json,
)
from pipelines.prontuarios.utils.tasks import (
    transform_filter_valid_cpf,
    transform_create_input_batches
)


def load_file_from_bucket(bucket, file):
    blob = bucket.blob(file)

    data = blob.download_as_string()

    df = pd.read_csv(StringIO(data.decode('utf-8')), dtype=str, index_col=0)

    return df


if __name__ == "__main__":
    
    inject_bd_credentials()

    client = storage.Client()
    bucket = client.get_bucket("prontuario-integrado")


    if not os.path.exists("tb_cns_provisorios.csv"):
        cns_data = load_file_from_bucket(bucket, "tb_cns_provisorios.csv")
        cns_data.to_csv("tb_cns_provisorios.csv")
    else:
        cns_data = pd.read_csv("tb_cns_provisorios.csv", dtype=str, index_col=0)
    print("CNS data loaded")

    if not os.path.exists("tb_pacientes.csv"):
        patient_data = load_file_from_bucket(bucket, "tb_pacientes.csv")
        patient_data.to_csv("tb_pacientes.csv")
    else:
        patient_data = pd.read_csv("tb_pacientes.csv", dtype=str, index_col=0)
    print("Patient data loaded")

    merged_data = transform_merge_patient_and_cns_data.run(patient_data, cns_data)
    print("Data merged")

    json_data = transform_data_to_json.run(merged_data)
    print("Data transformed to json")

    valid_data = transform_filter_valid_cpf.run(json_data)
    print("Data filtered")

    batches = transform_create_input_batches.run(valid_data)
