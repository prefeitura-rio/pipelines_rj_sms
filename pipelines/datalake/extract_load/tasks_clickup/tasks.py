# -*- coding: utf-8 -*-
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from pipelines.datalake.utils.data_transformations import convert_to_parquet
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def extract_clickup_list_tasks(
    clickup_personal_token,
    list_id,
):
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_tasks_with_pagination(page=0):
        response = requests.get(
            url=f"https://api.clickup.com/api/v2/list/{list_id}/task",
            headers={"Authorization": clickup_personal_token},
            params={
                "page": str(page),
                "subtasks": "true",
            },
        )
        response.raise_for_status()
        return response

    page = 0
    last_page = False

    tasks = []
    while not last_page:
        response = fetch_tasks_with_pagination(page=page)
        data = response.json()

        last_page = data.get("last_page", False)

        if not last_page:
            page += 1

        tasks.extend(response.json()["tasks"])

    task_data = pd.json_normalize(tasks, sep="_")
    task_data["datalake_loaded_at"] = pd.Timestamp.now()
    log(f"Extracted {len(task_data)} tasks from Clickup list {list_id}", level="info")

    task_data.to_csv("./task_data.csv", index=False, encoding="utf-8")

    return "./task_data.csv"
