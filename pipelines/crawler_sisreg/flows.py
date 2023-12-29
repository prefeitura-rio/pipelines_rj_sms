# -*- coding: utf-8 -*-
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.logging import log
from prefeitura_rio.pipelines_utils.bd import create_table_and_upload_to_gcs

from pipelines.constants import constants
from pipelines.crawler_sisreg.tasks import download_data

with Flow(
    name="SMS: Dump SISREG - Descrição do pipeline",
) as crawler_sisreg:

    # Tasks
    data = download_data()

    data.set_upstream()
    upload_to_datalake_task = create_table_and_upload_to_gcs(
        data_path=f"pipelines_rj_sms/pipelines/crawler_sisreg/data",
        dataset_id="brutos_regulacao_sisreg",
        table_id="crawler_sisreg",
        biglake_table=True,
    )
    upload_to_datalake_task.set_upstream(data)

# Storage and run configs
crawler_sisreg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
crawler_sisreg.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
