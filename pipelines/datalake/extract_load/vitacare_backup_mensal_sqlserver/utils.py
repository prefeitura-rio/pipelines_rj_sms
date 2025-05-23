# -*- coding: utf-8 -*-
import subprocess
import threading
import time

import prefect


def _pipe_output(stream, prefix, logger):
    for line in iter(stream.readline, b""):
        message = f"{prefix}: {line.decode(errors='replace').rstrip()}"
        logger.info(message)


def start_cloud_sql_proxy(connection_name: str) -> subprocess.Popen:
    logger = prefect.context.get("logger")
    logger.info(f"Starting Cloud SQL Proxy for connection: {connection_name}")

    # Testing if cloud-sql-proxy is installed
    subprocess.run(["cloud-sql-proxy", "--version"])

    process = subprocess.Popen(
        [
            "cloud-sql-proxy",
            connection_name,
            "--credentials-file=/tmp/credentials.json",
            "--port=1433",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
    )

    # Threads para logar stdout e stderr usando o logger do Prefect
    threading.Thread(
        target=_pipe_output, args=(process.stdout, "STDOUT", logger), daemon=True
    ).start()
    threading.Thread(
        target=_pipe_output, args=(process.stderr, "STDERR", logger), daemon=True
    ).start()

    time.sleep(5)

    return process


def stop_cloud_sql_proxy(proxy_process: subprocess.Popen):
    proxy_process.terminate()
    proxy_process.wait()
