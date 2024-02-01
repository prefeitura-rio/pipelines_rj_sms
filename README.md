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
            "name": "Projeto",
            "type": "debugpy",
            "request": "launch",
            "program": "run.py",
            "console": "integratedTerminal",
            "justMyCode": true,
            "env": {
                "INFISICAL_ADDRESS": <LOCALIZACAO>,
                "INFISICAL_TOKEN": <TOKEN>,
                "PREFECT__LOGGING__LEVEL": "DEBUG"
            }
        }
    ]
}
```

- Na raiz do projeto, crie um arquivo `run.py` que chame os flows que quer debugar.
- Exemplo:

```python
from pipelines.prontuarios.raw.smsrio.flows import sms_prontuarios_raw_smsrio

if __name__ == '__main__':
    sms_prontuarios_raw_smsrio.run(rename_flow=False)
```

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