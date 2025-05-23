# -*- coding: utf-8 -*-
import subprocess
import threading
import time

from pipelines.utils.logger import log


def _pipe_output(stream, prefix):
    for line in iter(stream.readline, b""):
        message = f"{prefix}: {line.decode(errors='replace').rstrip()}"
        print(message)


def start_cloud_sql_proxy(connection_name: str) -> subprocess.Popen:
    log(f"Starting Cloud SQL Proxy for connection: {connection_name}")

    cwd = [
        "cloud-sql-proxy",
        connection_name,
        "--credentials-file=/tmp/credentials.json",
        "--port=1433",
    ]

    process = subprocess.Popen(cwd, stdout=subprocess.PIPE)
    for line in process.stdout:
        log(line.decode())

    process.stdout.close()
    return_code = process.wait()

    if return_code:
        raise subprocess.CalledProcessError(return_code, cwd)

    return process



def stop_cloud_sql_proxy(proxy_process: subprocess.Popen):
    proxy_process.terminate()
    proxy_process.wait()
