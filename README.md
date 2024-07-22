# Pipelines RJ/SMS

## Setup

### Etapa 1 - Preparação de Ambiente
- Na raiz do projeto, prepare o ambiente:
 - `poetry shell`
 - `poetry install`

### Etapa 2 - Configuração de Debugging
- Crie pasta na raiz: `.vscode`
- Dentro da pasta, crie um arquivo: `launch.json` e coloque o seguinte conteúdo dentro dele:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Projeto",
            "type": "debugpy",
            "request": "launch",
            "program": "localrun.py",
            "console": "integratedTerminal",
            "justMyCode": false,
            "env": {
                "PREFECT__LOGGING__LEVEL": "INFO"
            }
        }
    ]
}
```
- Crie também um arquivo `.env` na raiz do projeto que defina as variáveis de ambiente: `INFISICAL_ADDRESS` e `INFISICAL_TOKEN`
    - Para preencher estes valores, entre em contato com o responsável pelo projeto.
- Agora este projeto fica disponível na aba de Debugging do VSCode para depuração.
- Quando se clica no ícone "Play", o script `localrun.py` será executado. Na etapa 3 iremos configurar essa etapa final do processo.

### Etapa 3 - Configurando Seleção de Casos
_Casos (inspirado em casos de teste) são combinações de flow com parâmetros especificos. Eles estão definidos no arquivo `localrun.cases.yaml`. Na hora de executar localmente, o script `localrun.py` procura qual caso está selecionado no arquivo `localrun.selected.yaml`._
- Crie um arquivo `localrun.selected.yaml` com conteúdo semelhante ao abaixo.

```yaml
selected_case: "report-data-ingestion"
override_params:
  environment: "dev"
```

- O arquivo acima diz que o caso de slug `report-data-ingestion` será executado quando iniciar a depuração. Além disso, ele especifica que o parametro "environment" será sobreposto para "dev". A sobreposição ocorre em relação à definição original do caso em `localrun.cases.yaml`.


### Etapa 4 (opcional) - Definindo Novos Casos
- Edite o arquivo `localrun.cases.yaml` criando novos casos. São os campos:
    - `case_slug`: Apelido do caso que serve como identificador.
    - `flow_path`: Caminho do módulo python em que o flow está definido. O mesmo usando no import do python.
    - `flow_name`: Nome da variável que o flow está armazenado dentro do módulo definido em `flow_path`.
    - `params`: Parâmetros que serão utilizados na execução do flow.
- O exemplo abaixo representa a definição de um caso:

```yaml
cases:
  - case_slug: "report-endpoint-health"
    flow_path: "pipelines.reports.endpoint_health.flows"
    flow_name: "disponibilidade_api"
    params:
      environment: "dev"
```

## Deploy em Staging
- Sempre trabalhe com branchs `staging/<nome>`
- Dê push e crie um Pull Request sem reviewer.
- Cada commit nesta branch irá disparar as rotinas do Github que:
 - Verificam formatação
 - Fazem Deploy
 - Registram flows em staging (ambiente de testes)
- Você acompanha o status destas rotinas na própria página do seu PR
- Flows registrados aparecem no servidor Prefect. Eles podem ser rodados por lá
