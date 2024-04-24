# -*- coding: utf-8 -*-
import re

import pandas as pd
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
        report.append(f"{row['time']} [{row['level'].rjust(5,' ')}] {row['text']}")
    report = "\n".join(report)
    log(f"Logs do DBT:{report}")

    with open("dbt_log.txt", "w+", encoding="utf-8") as log_file:
        log_file.write(report)

    return "dbt_log.txt"
