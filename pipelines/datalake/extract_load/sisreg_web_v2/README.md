# SISREG Web Scraper

## Descrição do Projeto

O intuito deste projeto é automatizar extração e carga dos diversos dados disponíveis no SISREG. O projeto é construído usando a biblioteca *Prefect* para orquestração e *Selenium* para Web Scraping. Os dados extraídos são transformados e carregados no nosso Data Lake (*Google Big Query*).

## Estrutura do Projeto

```
└── 📁sisreg_web_v2
    └── 📁sisreg
        └── 📁sisreg_components
            └── 📁core
                └── gerenciador_driver.py
                └── sisreg.py
            └── 📁pages
                └── base_page.py
                └── pagina_login.py
                └── pagina_oferta_programada.py
            └── 📁utils
                └── navegacao.py
                └── path_utils.py
    └── constants.py
    └── flows.py
    └── schedules.py
    └── tasks.py
    └── README.md
```

## Descrição dos Arquivos

- **gerenciador_driver.py**: Gerencia a configuração e instância do WebDriver do Firefox.
- **sisreg.py**: Classe principal que orquestra as interações com o SISREG, agregando e gerenciando diferentes páginas.
- **base_page.py**: Classe base para todas as páginas, fornecendo operações comuns como abrir URLs, lidar com frames e tratamento de exceções.
- **pagina_login.py**: Interage com a página de login do  SISREG.
- **pagina_oferta_programada.py**: Interage com a página responsável por baixar dados de ofertas programadas pelos profissionais de saúde.
- **navegacao.py**: Funções utilitárias para navegação em páginas web usando Selenium.
- **path_utils.py**: Funções utilitárias para manipulação de caminhos de arquivos.
- **constants.py**: Define valores constantes usados em todo o projeto.
- **flows.py**: Define o fluxo Prefect para extração e carga de dados do SISREG.
- **schedules.py**: Define os agendamentos para execução dos fluxos de extração e carga de dados do SISREG.
- **tasks.py**: Define as tarefas usadas no fluxo Prefect para extração e carga de dados do SISREG.
- **README.md**: Este arquivo.

## Contribuindo

Para contribuir com este projeto, você pode adicionar novas páginas ou métodos para extrair dados adicionais do site do SISREG. Aqui estão os passos a seguir:

### Adicionando Novas Páginas

1. **Crie uma nova classe de página**:
   - Crie um novo arquivo no diretório [`pages`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/pages), definindo uma nova classe. Cada classe deve representar uma página ou conjunto de dados do SISREG.
   - Implemente métodos para interagir com a nova página ou conjunto de dados.

2. **Atualize a classe [`Sisreg`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py)**:
   - Importe a nova classe de página em [`pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py).
   - Adicione a nova classe de página à classe [`Sisreg`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py), garantindo que ela seja herdada corretamente.

### Transformando suas interações em tarefas no Prefect

1. **Atualize o arquivo [`pipelines/datalake/extract_load/sisreg_web_v2/tasks.py`](pipelines/datalake/extract_load/sisreg_web_v2/tasks.py)**:
   - Defina novas tarefas que usem os novos métodos das suas novas classes.
   - Atualize o arquivo [`pipelines/datalake/extract_load/sisreg_web_v2/flows.py`](pipelines/datalake/extract_load/sisreg_web_v2/flows.py), garantindo que as tarefas que você criou sejam integradas corretamente ao fluxo do Prefect.

Seguindo esses passos, você pode estender a funcionalidade do projeto para extrairmos mais dados do SISREG! s2