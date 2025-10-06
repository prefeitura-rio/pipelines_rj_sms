from prefect import Parameter, case, unmapped, task
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.transform.gemini.procedimentos_sisreg.constants import constants as procedimentos_constants
from pipelines.datalake.transform.gemini.procedimentos_sisreg.schedules import procedimentos_monthly_update_schedule
from pipelines.datalake.transform.gemini.procedimentos_sisreg.tasks import get_result_gemini, parse_result_dataframe, reduce_raw_column
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, query_table_from_bigquery, rename_current_flow_run

with Flow(
    name="SISREG - Transformação - Padronização de procedimentos (Gemini)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.VITORIA_ID.value],
) as procedimentos_sisreg_flow:
    RENAME_FLOW = Parameter("rename_flow", default=False)
    ENVIRONMENT = Parameter("environment", default="prod")
    QUERY = Parameter("query", default=procedimentos_constants.QUERY.value)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(environment=ENVIRONMENT)

    query_results = query_table_from_bigquery(sql_query=QUERY, env=ENVIRONMENT)
    desc_interna, desc_grupo, codigos = reduce_raw_column(df=query_results)

    gemini_key = get_secret_key(
        secret_path=procedimentos_constants.INFISICAL_PATH.value,
        secret_name=procedimentos_constants.INFISICAL_API_KEY.value,
        environment=ENVIRONMENT,
    )

    list_result = get_result_gemini.map(
        procedimento=desc_interna,
        procedimento_grupo=desc_grupo,
        id_procedimento_sisreg=codigos,
        gemini_key=unmapped(gemini_key),
        model=unmapped(procedimentos_constants.GEMINI_MODEL.value),
    )

    df_result = parse_result_dataframe(list_result=list_result)

    @task(log_stdout=True)
    def build_update_sql_from_df(df):
        if df is None or getattr(df, "empty", True):
            return None
        def esc(v):
            return ("" if v is None else str(v)).replace("\\", "\\\\").replace("'", "\\'")
        selects = []
        for row in df.itertuples(index=False, name="Row"):
            cod = getattr(row, "id_procedimento_sisreg", None)
            pad = getattr(row, "procedimento_padronizado", None)
            if not cod or not pad:
                continue
            selects.append(f"SELECT '{esc(cod)}' AS id_procedimento_sisreg, '{esc(pad)}' AS procedimento_padronizado")
        if not selects:
            return None
        raw_union = "\n    UNION ALL\n    ".join(selects)
        project = getattr(constants, "PROJECT_ID", None)
        project = project.value if hasattr(project, "value") else (project or "rj-sms-dev")
        dataset = procedimentos_constants.DATASET_ID.value
        table = procedimentos_constants.TABLE_ID.value
        return f"""
        UPDATE `{project}.{dataset}.{table}` AS T
        SET procedimento_padronizado = S.procedimento_padronizado
        FROM (
        {raw_union}
        ) AS S
        WHERE T.id_procedimento_sisreg = S.id_procedimento_sisreg
        AND T.procedimento_padronizado IS NULL;
        """

    build_update_sql = build_update_sql_from_df(df_result)

    @task(log_stdout=True)
    def run_update_sql(sql_text: str):
        if not sql_text:
            return {"status": "no-op", "rows_affected": 0}
        from google.cloud import bigquery
        project = getattr(constants, "PROJECT_ID", None)
        project = project.value if hasattr(project, "value") else (project or "rj-sms-dev")
        dataset = procedimentos_constants.DATASET_ID.value
        client = bigquery.Client(project=project)
        try:
            dataset_ref = client.get_dataset(f"{project}.{dataset}")
            dataset_location = getattr(dataset_ref, "location", None)
        except Exception:
            dataset_location = None
        query_job = client.query(sql_text, location=dataset_location)
        query_job.result()
        try:
            affected = query_job.num_dml_affected_rows
        except Exception:
            affected = None
        return {"status": "ok", "rows_affected": affected, "location_used": dataset_location}

    run_update = run_update_sql(build_update_sql)

procedimentos_sisreg_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
procedimentos_sisreg_flow.executor = LocalDaskExecutor(num_workers=10)
procedimentos_sisreg_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
procedimentos_sisreg_flow.schedule = procedimentos_monthly_update_schedule