# -*- coding: utf-8 -*-
# pylint: disable= C0301
# flake8: noqa E501
import re

import pandas as pd
from dbt.contracts.results import RunResult, SourceFreshnessResult
from prefeitura_rio.pipelines_utils.logging import log


def process_dbt_logs(log_path: str = "dbt_repository/logs/dbt.log") -> pd.DataFrame:
    """
    Process the contents of a dbt log file and return a DataFrame containing the parsed log entries

    Args:
        log_path (str): The path to the dbt log file. Defaults to "dbt_repository/logs/dbt.log".

    Returns:
        pd.DataFrame: A DataFrame containing the parsed log entries.
    """

    with open(log_path, "r", encoding="utf-8", errors="ignore") as log_file:
        log_content = log_file.read()

    result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
    parts = [part.strip() for part in result][1:]

    splitted_log = []
    for i in range(0, len(parts), 2):
        time = parts[i].replace(r"\x1b[0m", "")
        level = parts[i + 1][1:6].replace(" ", "")
        text = parts[i + 1][7:]
        splitted_log.append((time, level, text))

    full_logs = pd.DataFrame(splitted_log, columns=["time", "level", "text"])

    return full_logs


def log_to_file(logs: pd.DataFrame, levels=None) -> str:
    """
    Writes the logs to a file and returns the file path.

    Args:
        logs (pd.DataFrame): The logs to be written to the file.
        levels (list): The levels of logs to be written to the file.

    Returns:
        str: The file path of the generated log file.
    """
    if levels is None:
        levels = ["info", "error", "warn"]
    logs = logs[logs.level.isin(levels)]

    report = []
    for _, row in logs.iterrows():
        report.append(f"{row['time']} [{row['level'].rjust(5, ' ')}] {row['text']}")
    report = "\n".join(report)
    log(f"Logs do DBT:{report}")

    with open("dbt_log.txt", "w+", encoding="utf-8") as log_file:
        log_file.write(report)

    return "dbt_log.txt"


# =============================
# SUMMARIZERS
# =============================


class RunResultSummarizer:
    """
    A class that summarizes the result of a DBT run.

    Methods:
    - summarize(result): Summarizes the result based on its status.
    - error(result): Returns an error message for the given result.
    - fail(result): Returns a fail message for the given result.
    - warn(result): Returns a warning message for the given result.
    """

    def summarize(self, result):
        if result.status == "error":
            return self.error(result)
        elif result.status == "fail":
            return self.fail(result)
        elif result.status == "warn":
            return self.warn(result)

    def error(self, result):
        return f"`{result.node.name}`\n  {result.message.replace('__', '_')} \n"

    def fail(self, result):
        return f"`{result.node.name}`\n   {result.message}: ``` select * from {result.node.relation_name.replace('`','')}``` \n"  # noqa

    def warn(self, result):
        return f"`{result.node.name}`\n   {result.message}: ``` select * from {result.node.relation_name.replace('`','')}``` \n"  # noqa


class FreshnessResultSummarizer:
    """
    A class that summarizes the freshness result of a DBT node.

    Methods:
    - summarize(result): Summarizes the freshness result based on its status.
    - error(result): Returns the error message for a failed freshness result.
    - fail(result): Returns the name of the failed freshness result.
    - warn(result): Returns the warning message for a stale freshness result.
    """

    def summarize(self, result):
        if result.status == "error":
            return self.error(result)
        elif result.status == "fail":
            return self.fail(result)
        elif result.status == "warn":
            return self.warn(result)

    def error(self, result):
        freshness = result.node.freshness
        error_criteria = f">={freshness.error_after.count} {freshness.error_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({error_criteria})"

    def fail(self, result):
        return f"{result.node.relation_name.replace('`', '')}"

    def warn(self, result):
        freshness = result.node.freshness
        warn_criteria = f">={freshness.warn_after.count} {freshness.warn_after.period}"
        return f"{result.node.relation_name.replace('`', '')}: ({warn_criteria})"


class Summarizer:
    """
    A class that provides summarization functionality for different result types.
    This class can be called with a result object and it will return a summarized version
        of the result.

    Attributes:
        None

    Methods:
        __call__: Returns a summarized version of the given result object.

    """

    def __call__(self, result):
        if isinstance(result, RunResult):
            return RunResultSummarizer().summarize(result)
        elif isinstance(result, SourceFreshnessResult):
            return FreshnessResultSummarizer().summarize(result)
        else:
            raise ValueError(f"Unknown result type: {type(result)}")
