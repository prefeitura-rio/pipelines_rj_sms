# -*- coding: utf-8 -*-
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_request(url, headers):
    session = requests.Session()
    retries = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 104])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    try:
        response = session.get(url=url, headers=headers)
    except requests.exceptions.RequestException:
        return None
    return response
