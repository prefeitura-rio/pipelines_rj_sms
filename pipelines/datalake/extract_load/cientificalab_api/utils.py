from datetime import datetime, timedelta
import pytz


def generate_3_windows():
    tz = pytz.timezone("America/Sao_Paulo")
    yesterday = (datetime.now(tz) - timedelta(days=1)).date()

    base_date_str = yesterday.strftime("%Y-%m-%d")

    janelas = [
        ("00:00:01", "08:00:00"),
        ("08:00:01", "16:00:00"),
        ("16:00:01", "23:59:59"),
    ]

    parametros = []
    for inicio, fim in janelas:
        parametros.append({
            "dt_inicio": f"{base_date_str}T{inicio}-0300",
            "dt_fim": f"{base_date_str}T{fim}-0300",
            "environment": "prod",  
        })

    return parametros