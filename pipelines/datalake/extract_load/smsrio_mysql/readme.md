# Flow de Extração de Dados - SMSRIO / MySQL

## Parâmetros
- table_id* (str): representa o nome da tabela em que o dado será armazenado.
- dataset_id* (str): representa o nome do dataset em que o dado será armazenado.
- environment* (str): representa o ambiente das secrets. `prod`, `dev`, `staging` ou `local-prod`.
- rename_flow* (bool): indica se deve renomear a flow run. Útil apenas no Prefect UI.
- relative_date_filter (date): representa a data em que o dado da fonte será filtrado.
- schema* (str): representa o nome do schema MySQL em que a tabela fonte se encontra.
- partition_column (str): nome da coluna que será utilizada para criar as partições do dataframe.
- id_column (str): especifica o id da coluna que será utilizada para ordenamento dos dados. Padrão `id`
- datetime_column(str): especifica o nome da coluna temporal que será utilizada para filtro. Padrão `timestamp`
