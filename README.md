# Pipelines RJ/SMS

## Setup

### Preparação de Ambiente
- Na raiz do projeto, prepare o ambiente:
 - `poetry shell`
 - `poetry install`
- Na raiz do projeto, fora do ambiente virtual:
 - `pip install flake8`

### Debugging
- Crie pasta na raiz: `.vscode`
- Dentro da pasta, crie um arquivo: `launch.json` e coloque o seguinte conteúdo dentro dele:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug: Flows",
            "type": "debugpy",
            "request": "launch",
            "program": "local_run.py",
            "args": [
                "--case", "0"
            ],
            "console": "integratedTerminal",
            "justMyCode": true,
            "env": {
                "INFISICAL_ADDRESS": <INFISICAL_ADDRESS>,
                "INFISICAL_TOKEN": <INFISICAL_TOKEN>,
                "PREFECT__LOGGING__LEVEL": "DEBUG"
            }
        }
    ]
}
```
- Agora este projeto fica disponível na aba de Debugging.

## Rodando (local)
- Para rodar basta usar o Debugger do VSCode, que vai detectar automaticamente a configuração feita.

## Deploy em Staging
- Sempre trabalhe com branchs `staging/<nome>`
- Dê push e crie um Pull Request sem reviewer.
- Cada commit nesta branch irá disparar as rotinas do Github que:
 - Verificam formatação
 - Fazem Deploy
 - Registram flows em staging (ambiente de testes)
- Você acompanha o status destas rotinas na própria página do seu PR
- Flows registrados aparecem no servidor Prefect. Eles podem ser rodados por lá