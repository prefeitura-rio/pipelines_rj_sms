# -*- coding: utf-8 -*-
from datetime import timedelta
from functools import partial

from prefect import task
from prefect.engine import state
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.utils.state_handlers import on_fail, on_success


@task
def log_error():
    print("ERROR")


@task
def log_success():
    print("SUCCESS")


@task
def soma_um(x: int) -> int:
    return x + 1


@task
def soma_dois(x: int) -> int:
    return x + 1


def notify_on_retry(task, old_state, new_state, new_task):
    if isinstance(new_state, state.Failed):
        print(f"Task {task} failed, retrying...")
        new_task.run()
    return new_state


@task(state_handlers=[partial(on_fail, task_to_run_on_fail=log_error)])
def error_task():
    create_flow_run.run()


with Flow(name="Teste") as sub_flow:
    soma_dois(4)


with Flow(name="Teste") as flow:
    x = soma_um(1)
    y = soma_dois(x)
    subflow = error_task()

    wait_for_reprocessing = wait_for_flow_run(
        subflow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
        max_duration=timedelta(seconds=90),
    )


task_ref = flow.get_tasks()[0]

state = flow.run()
print(state._result.value)
print(state.result[task_ref]._result.value)
