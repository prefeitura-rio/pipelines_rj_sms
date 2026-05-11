# -*- coding: utf-8 -*-
# pylint: disable=import-error
"""
ArcGIS FeatureServer extract/load flow.
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.arcgis.constants import (
    constants as arcgis_constants,
)
from pipelines.datalake.extract_load.arcgis.schedules import (
    arcgis_feature_server_combined_schedule,
)
from pipelines.datalake.extract_load.arcgis.tasks import (
    build_arcgis_dataframes,
    fetch_arcgis_layer_features,
    fetch_arcgis_layer_metadata,
    get_last_loaded_layer_hash,
    resolve_changed_dataframes,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import upload_df_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - ArcGIS FeatureServer",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DAYANE_ID.value,
    ],
) as datalake_extract_arcgis:

    ENDPOINT_URL = Parameter("endpoint_url", required=True)
    DATASET_ID = Parameter("dataset_id", required=True)
    TABLE_ID = Parameter("table_id", required=True)
    VERSIONS_TABLE_ID = Parameter(
        "versions_table_id",
        default=arcgis_constants.VERSIONS_TABLE_ID.value,
    )
    WHERE_CLAUSE = Parameter("where_clause", default=arcgis_constants.DEFAULT_WHERE.value)
    OUT_FIELDS = Parameter("out_fields", default=arcgis_constants.DEFAULT_OUT_FIELDS.value)
    PAGE_SIZE = Parameter("page_size", default=arcgis_constants.DEFAULT_PAGE_SIZE.value)
    OUTPUT_FORMAT = Parameter("output_format", default=arcgis_constants.DEFAULT_FORMAT.value)
    ENVIRONMENT = Parameter("environment", default="prod")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    IF_EXISTS = Parameter("if_exists", default="append")
    IF_STORAGE_DATA_EXISTS = Parameter("if_storage_data_exists", default="append")
    DUMP_MODE = Parameter("dump_mode", default="append")

    metadata = fetch_arcgis_layer_metadata(
        endpoint_url=ENDPOINT_URL,
    )

    features = fetch_arcgis_layer_features(
        endpoint_url=ENDPOINT_URL,
        where_clause=WHERE_CLAUSE,
        out_fields=OUT_FIELDS,
        page_size=PAGE_SIZE,
        output_format=OUTPUT_FORMAT,
    )

    dataframes = build_arcgis_dataframes(
        features=features,
        metadata=metadata,
        endpoint_url=ENDPOINT_URL,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )

    last_loaded_hash = get_last_loaded_layer_hash(
        dataset_id=DATASET_ID,
        versions_table_id=VERSIONS_TABLE_ID,
        endpoint_url=ENDPOINT_URL,
        environment=ENVIRONMENT,
    )

    resolved = resolve_changed_dataframes(
        dataframes=dataframes,
        last_loaded_hash=last_loaded_hash,
    )

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            dataset=DATASET_ID,
            table=TABLE_ID,
        )

    features_upload = upload_df_to_datalake(
        df=resolved["features"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        source_format="parquet",
        if_exists=IF_EXISTS,
        if_storage_data_exists=IF_STORAGE_DATA_EXISTS,
        dump_mode=DUMP_MODE,
    )

    version_upload = upload_df_to_datalake(
        df=resolved["version"],
        dataset_id=DATASET_ID,
        table_id=VERSIONS_TABLE_ID,
        source_format="parquet",
        if_exists=IF_EXISTS,
        if_storage_data_exists=IF_STORAGE_DATA_EXISTS,
        dump_mode=DUMP_MODE,
        upstream_tasks=[features_upload],
    )


datalake_extract_arcgis.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datalake_extract_arcgis.executor = LocalDaskExecutor(num_workers=4)
datalake_extract_arcgis.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
    memory_request="4Gi",
)
datalake_extract_arcgis.schedule = arcgis_feature_server_combined_schedule