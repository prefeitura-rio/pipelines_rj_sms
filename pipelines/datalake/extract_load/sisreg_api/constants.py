# -*- coding: utf-8 -*-
"""
Constantes
"""

CONFIG = {
    "host": "sisreg-es.saude.gov.br",
    "port": 443,
    "scheme": "https",
    "flow_name": "SISREG API",
    "flow_owner": "matheusmiloski",
    "partition_column": "data_extracao",
    "source_format": "parquet",
    "memory_request": "12Gi",
    "memory_limit": "12Gi",
    "num_workers": 3,
}
