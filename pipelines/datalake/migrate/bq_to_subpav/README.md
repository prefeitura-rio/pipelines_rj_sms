# DataLake - Migration - BigQuery to MySQL SUBPAV

Pipeline para migração controlada de dados entre tabelas do BigQuery e MySQL para uso exclusivo da SUBPAV.

## Avisos Importantes

* **Apenas usuários autorizados:** Para qualquer solicitação de inserção, é necessário:

  * Autorização formal da SUBPAV.
  * Permissão individual do usuário no banco de dados (com GRANTs apropriados).
  * Cadastro do segredo de conexão no INFISICAL.

---

## Parâmetros

### **BigQuery**

* **dataset\_id**: Dataset de origem no BigQuery.
* **table\_id**: Tabela de origem no BigQuery.
* **bq\_columns**: Lista de colunas a extrair. Permite alias (ex: `col as nome_col`).
* **limit**: Limite de registros a extrair (opcional).

### **MySQL**

* **db\_schema**: Schema (database) de destino no MySQL.
* **dest\_table**: Nome da tabela de destino.
* **if\_exists**: Como tratar existência da tabela (`append`, `replace`, etc.).
* **custom\_insert\_query**: Query SQL customizada para inserção.
* **infisical\_path**: Caminho no INFISICAL.
* **secret\_name**: Nome do segredo de conexão no INFISICAL.

### **Flow/Execução**

* **project**: Nome ou descrição do projeto/processo.
* **environment**: Ambiente de execução (`dev`, `prod`, etc.).
* **notify**: Ativa/desativa notificação (Discord/DS).
* **rename\_flow**: Renomeia dinamicamente a execução no Prefect.
* **custom\_insert\_query**: Query personalizada (ver exemplos abaixo).

### **Agendamento**

* **frequency**: Frequência do schedule (`daily`, `weekly`, `monthly`).

---

## Exemplos de Agendamentos

```python
TABLES_CONFIG = [
    # 1. Carga diária padrão - apenas parâmetros obrigatórios
    {
        "project": "Carga Diária",
        "dataset_id": "dataset_exemplo",
        "table_id": "tabela_origem",
        "db_schema": "schema_destino",
        "dest_table": "tabela_destino",
        "frequency": "daily",
        "infisical_path": "/caminho_secret",
        "secret_name": "MYSQL_URL",
        # Parâmetros opcionais abaixo:
        # "bq_columns": None,         # extrai todas as colunas se não especificar
        # "if_exists": "append",      # default: append
        # "notify": True,             # default: False
        # "environment": "prod",      # default: dev
        # "rename_flow": False,       # default: False
        # "limit": None,              # default: sem limite
        # "custom_insert_query": None # default: insert padrão
    },
    # 2. Carga semanal resumida - usando colunas específicas, limite e notify desativado
    {
        "project": "Carga Semanal Resumida",
        "dataset_id": "dataset_semanal",
        "table_id": "tabela_semanal",
        "db_schema": "schema_semana",
        "dest_table": "tabela_semana",
        "frequency": "weekly",
        "infisical_path": "/caminho_secret",
        "secret_name": "MYSQL_URL",
        "bq_columns": ["id", "campo_a", "campo_b"], # opcional
        "limit": 1000,                              # opcional
        "notify": False,                            # opcional
    },
    # 3. Carga mensal customizada - com query de inserção personalizada para evitar duplicidade
    {
        "project": "Carga Mensal Customizada",
        "dataset_id": "dataset_mensal",
        "table_id": "tabela_mensal",
        "db_schema": "schema_mensal",
        "dest_table": "tabela_destino_mensal",
        "frequency": "monthly",
        "infisical_path": "/caminho_secret",
        "secret_name": "MYSQL_URL",
        "bq_columns": ["id", "campo_x", "campo_y"],      # opcional
        "custom_insert_query": """
            INSERT INTO tabela_destino_mensal (id, campo_x, campo_y)
            SELECT :id, :campo_x, :campo_y
            WHERE NOT EXISTS (
                SELECT 1 FROM tabela_destino_mensal
                WHERE id = :id
            )
        """,                                              # opcional
        "notify": True,                                   # opcional
    },
]
```

---

**Dúvidas ou solicitações:**

[suporte@subpav.org](mailto:suporte@subpav.org)

