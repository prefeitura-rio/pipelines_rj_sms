# 📊 Pipelines **RJ‑SMS**

> Ambiente para extração e carga de dados brutos, em **Python + Prefect**, que abastece o Data Lake (**Google Cloud Storage / Google Big Query**) da Secretaria Municipal de Saúde do Rio de Janeiro (**SMS Rio**).

Administrador: [Pedro Marques](@TanookiVerde)


---

## 🛠️ Pré-requisitos

| Ferramenta | Versão | Observações |
|------------|--------|-------------|
| **Python** | 3.10.x | Windows: Baixe o instalador https://www.python.org/downloads/release/python-3109/ |
| **Poetry** | 1.7.1  | `pip install poetry==1.7.1` |
| **Git** | | Windows: Baixe o instalador https://git-scm.com/downloads/win |

> **Clone o repositório**
> ```bash
> git clone https://github.com/prefeitura-rio/pipelines_rj_sms
> cd pipelines_rj_sms
> ```

---

## ⚙️ Configuração passo a passo

### 1 - Ambiente Python
Dentro do repositório, execute:

```bash
poetry shell             # cria/ativa o venv isolado
poetry install           # instala todas as dependências declaradas em pyproject.toml
```

O comando poetry shell garante que as libs sejam instaladas no ambiente virtual correto, evitando conflitos.

> No futuro, caso precise de alguma lib extra, será necessário executar `poetry add <pacote>`.
> Nestes casos, fale com o administrador do projeto.

### 2 - Variáveis de Ambiente

Crie um arquivo `.env` na raiz do repositório:

```env
INFISICAL_ADDRESS=xxxxxxxxxxxxxxxx
INFISICAL_TOKEN=xxxxxxxxxxxxxxxx
```

Peça as credenciais do Infisical ao administrador do projeto.

### 3 - VS Code & Debug

1. Crie a pasta `.vscode` também na raiz.
2. Dentro da pasta, crie o arquivo `launch.json` com o seguinte conteúdo:

```jsonc
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

Agora este projeto fica disponível na aba de Debugging do VSCode para depuração.
Quando se clica no ícone *Play*, o script `localrun.py` será executado.
Na próxima etapa iremos configurar isso.

### 4 - Configurando Seleção de Casos
> Casos (inspirado em casos de teste) são combinações de flow com parâmetros especificos. Eles estão definidos no arquivo `localrun.cases.yaml`. Na hora de executar localmente, o script `localrun.py` procura qual caso está selecionado no arquivo `localrun.selected.yaml`.

- Crie um arquivo `localrun.selected.yaml` com conteúdo semelhante ao abaixo.

```yaml
selected_case: "report-data-ingestion"
override_params:
  environment: "dev"
```

- O arquivo acima diz que o caso de slug `report-data-ingestion` será executado quando iniciar a depuração. Além disso, ele especifica que o parametro "environment" será sobreposto para "dev". A sobreposição ocorre em relação à definição original do caso em `localrun.cases.yaml`.

- Cheque se seu ambiente está executando com o compilador certo (Python 3.10.x)


### 5 - (Opcional) Definindo Novos Casos
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

---

## 🤝 Como contribuir com o projeto
Não esqueça de checar se você está logado no seu ambiente com a sua conta certa do GitHub.
(A que você quer usar para trabalhar nesse projeto).



1. **Branch:** Sempre abra suas branchs de trabalho no formato `staging/<sua-feature>`. O título deve sempre que possível respeitar a estrutura: [`<PROJETO>`] `<acao>`: `<breve_descricao>`.
2. **Commits semânticos:** Utilize commits no formato `feat: <breve_descricao>`, `fix: <breve_descricao>`, `chore: <breve_descricao>`, etc
3. Abra **Pull Request** sem _reviewer_.

> - Cada commit nesta branch irá disparar as rotinas do Github que:
>    - Verificam formatação
>    - Fazem Deploy
>    - Registram flows em staging (ambiente de testes)
>
> - Você acompanha o status destas rotinas na própria página do seu PR
> - Flows registrados aparecem no servidor Prefect. Eles podem ser rodados por lá
> - Ao rodar os flows no Prefect, você poderá ver os dados pelo Big Query.

5. Valide no Prefect UI o funcionamento do flow usando o projeto "staging", sempre com 'dev' como environment.

> **Obs** Solicite o endereço e credenciais do Prefect UI ao administrador do projeto.

6. Com execução validada, adicione o administrador (TanookiVerde) como reviewer do PR e mande uma mensagem no servidor do Discord da DIT.

**Teste e verifique os dados!**

---

## Qualquer dúvida, erro, crítica ou sugestão:
### Basta entrar em contato com o [Administrador](@TanookiVerde) ❤️