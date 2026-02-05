

# Atualmente só para referência
flow_parameters = [
    {
        "environment": "prod",
        "endpoint": '/resultadoprepperiodo/',
        "interval": 5, # intervalo em anos para extração
        "year": 2025, # ano de referência para extração
        "month": "06", # mês de referência para extração (não utilizado em extrações anuais)
        "annual": False, # define se a extração é anual ou mensal
        "dataset_id": "brutos_siclom_api", # ID do dataset no Data Lake
        "table_id": "teste_pre" # ID da tabela no Data Lake
    },
    {
        "environment": "prod",
        "endpoint": '/mostracargacd4periodo/',
        "interval": 5,
        "year": 2025,
        "month": "06",
        "annual": False,
        "dataset_id": "brutos_siclom_api",
        "table_id": "teste_pre"
    }
]