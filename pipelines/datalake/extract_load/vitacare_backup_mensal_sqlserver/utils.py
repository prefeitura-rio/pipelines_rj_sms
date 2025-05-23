import subprocess
import time
from pipelines.utils.logger import log


def start_cloud_sql_proxy(connection_name: str) -> subprocess.Popen:
    log(f"Starting cloud SQL proxy for connection name: {connection_name}")
    log(f"[IMPORTANT] Please guarantee that cloud-sql-proxy is installed in the system")

    proxy_process = subprocess.Popen([
        "cloud-sql-proxy",
        connection_name,
        "--credentials-file=/tmp/credentials.json",
        "--port=1433",
    ])

    # espera proxy levantar
    time.sleep(5)  

    return proxy_process

def stop_cloud_sql_proxy(proxy_process: subprocess.Popen):
    proxy_process.terminate()
    proxy_process.wait()
    
    return