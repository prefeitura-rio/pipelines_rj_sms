# Flow de Extração de Dados - SMSRIO / MySQL

- Objetivo:

## Parâmetros
- table_id* (str): representa o nome da tabela em que o dado será armazenado.
- dataset_id* (str): representa o nome do dataset em que o dado será armazenado.
- environment* (str): representa o ambiente das secrets. `prod`, `dev`, `staging` ou `local-prod`.
- rename_flow* (bool): indica se deve renomear a flow run. Útil apenas no Prefect UI.
- relative_date_filter (date): representa a data em que o dado da fonte será filtrado.
- schema* (str): representa o nome do schema MySQL em que a tabela fonte se encontra.

## Casos de Uso

### Extração sem Filtro
- Extrai uma tabela de forma completa, sem filtrar por data

```
{
    "table_id": "contatos_unidades",
    "dataset_id": "brutos_plataforma_smsrio",
    "environment": "prod",
    "rename_flow": True,
    "relative_date_filter": None,
    "schema": "subpav_cnes",
}
```

### Extração com Filtro de Data
- Extrai uma tabela de forma incremental, filtrando por data

```
{
    "table_id": "contatos_unidades",
    "dataset_id": "brutos_plataforma_smsrio",
    "environment": "prod",
    "rename_flow": True,
    "relative_date_filter": "2025-01-01",
    "schema": "subpav_cnes",
}
```
