# -*- coding: utf-8 -*-
# pylint: disable=W0401, W0614, W0613
# flake8: noqa: F401, F403

"""
State handlers for prefect tasks
"""

from prefect.engine import state


def on_fail(task, old_state, new_state, task_to_run_on_fail):
    """
    Handles the behavior when a task fails.

    Args:
        task (str): The name of the task that failed.
        old_state: The previous state of the task.
        new_state: The new state of the task.
        task_to_run_on_fail: The task to run when the specified task fails.

    Returns:
        The new state of the task.
    """
    if isinstance(new_state, state.Failed):
        print(f"Task {task} failed...")
        task_to_run_on_fail.run()
    return new_state


def on_success(task, old_state, new_state, task_to_run_on_success):
    """
    Handles the logic when a task transitions to a success state.

    Args:
        task: The task that transitioned to the success state.
        old_state: The previous state of the task.
        new_state: The new success state of the task.
        task_to_run_on_success: The task to run when the success state is reached.

    Returns:
        The new state of the task.
    """

    if isinstance(new_state, state.Success):
        print(f"Task {task} Success...")
        task_to_run_on_success.run()
    return new_state

