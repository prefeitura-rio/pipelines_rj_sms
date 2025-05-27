# -*- coding: utf-8 -*-
import socket
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

    process = subprocess.Popen(cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    threading.Thread(
        target=_pipe_output, args=(process.stdout, "cloud-sql-proxy"), daemon=True
    ).start()

    return process  # Devolve o processo imediatamente


def stop_cloud_sql_proxy(proxy_process: subprocess.Popen):
    proxy_process.terminate()
    proxy_process.wait()


def wait_for_proxy(port: int, host: str = "localhost", timeout: int = 30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Proxy did not become available at {host}:{port} within {timeout} seconds.")
