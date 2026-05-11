# ProntuaRIO - Extração e Carga de Backups (GCS)

Esta pasta contém os pipelines (flows) e tarefas (tasks) responsáveis pela extração e carga de dados de backups do sistema ProntuaRIO (OpenBase e Postgres) armazenados no Google Cloud Storage (GCS) para o Datalake.

## Arquitetura dos Flows (`flows.py`)

A arquitetura utiliza o padrão **Manager-Operator** do Prefect.

### 1. Manager: `DataLake - Extração e Carga de Dados - ProntuaRIO Backups (MANAGER)`
- **Objetivo:** Orquestrar o processamento de todos os arquivos de backup de uma determinada competência/pasta.
- **Funcionamento:**
  - Lista os arquivos de backup presentes no bucket GCS (pasta fornecida ou o mês/ano atual).
  - Identifica e estrutura os parâmetros para cada CNES encontrado.
  - Executa dinamicamente as flows do tipo *Operator* para cada tipo de banco (OpenBase e Postgres) mapeando os processamentos.
  - Aguarda a conclusão de todos os operators iniciados.

### 2. Operator OpenBase: `DataLake - Extração e Carga de Dados - ProntuaRIO Backups OpenBase (OPERATOR)`
- **Objetivo:** Extrair e carregar dados estruturados contidos nos backups do OpenBase.
- **Funcionamento:**
  - Cria os diretórios temporários necessários para o processamento.
  - Realiza o download do arquivo de backup específico do OpenBase.
  - Descompacta o arquivo (.tar/.tar.bz2).
  - Extrai as tabelas em *chunks*, converte e envia os dados extraídos para o Datalake.
  - Remove os diretórios temporários (limpeza do ambiente).

### 3. Operator Postgres: `DataLake - Extração e Carga de Dados - ProntuaRIO Backups Postgres (OPERATOR)`
- **Objetivo:** Extrair e carregar dados estruturados do arquivo de dump do Postgres (`hospub.sql`).
- **Funcionamento:**
  - Cria as pastas temporárias.
  - Baixa o pacote de backup que contém o arquivo do Postgres.
  - Descompacta seletivamente o arquivo `hospub.sql`.
  - Processa o `.sql` em chunks extraindo as tabelas selecionadas e fazendo a carga para o Datalake.
  - Limpa os diretórios temporários.

---

## Tarefas (`tasks.py`)

As principais *tasks* que compõem esses flows incluem:

- **Gerenciamento de Arquivos/Diretórios Temporários:**
  - `create_temp_folders(folders)`: Inicializa a árvore de pastas locais temporárias necessárias ao processamento do worker.
  - `delete_temp_folders(folders, wait_for)`: Limpa as pastas do worker após o fim da extração, evitando superlotação de disco (disco efêmero).

- **Operações no Cloud Storage:**
  - `list_files_from_bucket(environment, bucket_name, folder)`: Acessa o bucket e lista os arquivos contidos, essencial para o mapeamento dos CNES pelo *Manager*.
  - `get_file(...)`: Realiza o download de arquivos específicos para a máquina de processamento local.

- **Processamento e Extração de Dados:**
  - `unpack_files(...)`: Descompacta os arquivos `.tar` e `.tar.bz2`, suportando a exclusão dos arquivos originais para economizar espaço em disco.
  - `extract_postgres_data(...)`: Executa a leitura de arquivos dump `.sql` (como o hospub), interpretando e convertendo as tabelas relevantes em pedaços (*chunks*) para reduzir custo de RAM.
  - `extract_openbase_data(...)`: Processa os dados do tipo OpenBase extraindo informações tabulares e convertendo-as para as cargas *chunked*.

- **Orquestração e Integração no DataLake:**
  - `build_openbase_parameters(...)` / `build_postgres_parameters(...)`: Estruturam os argumentos das Flows (operators) no Manager.
  - `upload_file_to_datalake(...)` / `upload_file_to_native_table(...)`: Disparam o upload dos chunks processados direto para o BigQuery/Datalake de forma particionada e segura.
  - `generate_current_folder(folder)`: Estima a competência atual por padrão (mês/ano) caso não tenha sido parametrizada (ex: extrações semanais contínuas).

---

## Utilitários e Auxiliares (`utils.py`)

O módulo `utils.py` concentra funções essenciais para a interpretação e manipulação dos dados brutos, suportando as *tasks* em processos complexos.

- **Processamento de Bancos OpenBase (Formatos Específicos e Binários):**
  - **Funções de conversão de tipos:** `handle_J`, `handle_U`, `handle_D`, `handle_others`: Converte representações de bytes para dados interpretáveis dependendo dos atributos e tipos do campo (strings, decimais, datas).
  - **Metadados e Dicionários:** `find_openbase_folder`, `get_table_and_dictionary_files`, `parse_dictionary_file`, `load_all_dictionaries`, `get_metadata_info`: Varrem as estruturas locais identificando diretórios dos bancos, lêem e parseiam os dicionários de tabelas que dão o esqueleto para decodificar os arquivos binários.
  - **Extração e Gravação CSV:** `extract_field_value`, `parse_record`, `write_csv_header`, `openbase_write_csv_row`: Desempacotam campos individuais a partir de registros em bytes (`rec: bytes`) e formatam em saídas CSV padronizadas prontas para *chunking* e upload.

- **Processamento de Banco de Dados PostgreSQL (.sql em dump):**
  - **Manipulação de comandos INSERT:** `extract_table_name`, `extract_columns`, `extract_values`: Efetuam *parsing* em cláusulas SQL puras (como de dumps) para extrair os componentes de um `INSERT INTO`, transformando os dados contidos em SQL para tabelas brutas/CSV.
  - **Limpeza de Strings:** `clean_value`: Trata a formatação de *strings* retirando aspas duplas/simples extremas e escapando as internas, garantindo arquivos CSV finais seguros.
  - **Fluxo contínuo (Streaming):** `process_insert_statement`, `process_sql_file_streaming`: Processam dumps imensos lendo arquivos linha-a-linha por *buffer* (`buffer_size`), evitando falhas de exaustão de memória na leitura (Out Of Memory).