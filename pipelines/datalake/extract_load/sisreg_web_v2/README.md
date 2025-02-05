# SISREG Web Scraper

## Descrição do Projeto

O intuito deste projeto é automatizar a Extração e Carga dos diversos dados disponíveis no website do SISREG (Sistema Nacional de Regulação). O projeto é construído utilizando bibliotecas Python:
- **Prefect** para orquestração
- **Selenium** para interação com as páginas  
  
Os dados extraídos são carregados no nosso Data Lake (**Google Big Query**).  

A modelagem do projeto é inspirada em **Page Object Models** (POM), onde cada página do website é representada por uma classe. As interações com a página são definidas como métodos dessas classes. Por fim, existe uma classe central que herda todas as páginas individuais e portanto serve de interface, por onde é possível interagir com o website de maneira simplificada. O objetivo desta modularização é ganhar eficiência e produtividade.    

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

- **gerenciador_driver.py**: Instancia o WebDriver do Firefox e gerencia suas configurações.
- **sisreg.py**: Classe central que orquestra as interações com o SISREG, agregando e gerenciando diferentes páginas.
- **base_page.py**: Classe base para todas as páginas. Nesta classe são definidas operações/interações que podem se repetir nas diferentes páginas.
- **pagina_login.py**: Interage com a página de login do  SISREG.
- **pagina_oferta_programada.py**: Interage com a página responsável por baixar dados de ofertas programadas pelos profissionais de saúde.
- **navegacao.py**: Funções utilitárias para navegação em páginas web usando Selenium (podem ser usadas em diferentes etapas do projeto). 
- **path_utils.py**: Funções utilitárias para manipulação de caminhos de arquivos (podem ser usadas em diferentes etapas do projeto).
- **constants.py**: Define os valores constantes que serão utilizados no projeto.
- **flows.py**: Define o fluxo Prefect para extração e carga de dados do SISREG.
- **schedules.py**: Define os agendamentos para execução dos fluxos.
- **tasks.py**: Define as tarefas específicas usadas no fluxo.
- **README.md**: Este arquivo.

## Contribuindo

Para contribuir com este projeto você pode adicionar novas páginas ou métodos para extrair dados adicionais do site do SISREG. Aqui estão os passos a seguir:

### Adicionando Novas Páginas

1. **Crie uma nova classe de página**:
   - Crie um novo arquivo *.py* no diretório [`pages`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/pages), definindo uma nova classe. Cada classe deve representar uma página ou conjunto de dados do SISREG.
   - Implemente métodos para interagir com a nova página ou conjunto de dados.
   - Utilize nomes de métodos descritivos, de maneira à garantir que sejam únicos, e não ocorram conflitos com métodos definidos em outras páginas.

2. **Atualize a classe [`Sisreg`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py)**:
   - Importe sua nova classe de página em [`pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py).
   - Adicione esta nova classe de página à classe central [`Sisreg`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py), garantindo que ela seja herdada corretamente.

3. **Se necessário, contribua para [`utils`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/utils)**
    - Aqui você definirá funções comuns às diversas etapas do projeto, evitando repetição de código.


### Transformando suas interações em tarefas no Prefect

1. **Atualize o arquivo [`pipelines/datalake/extract_load/sisreg_web_v2/tasks.py`](pipelines/datalake/extract_load/sisreg_web_v2/tasks.py)**:
   - Defina as novas tarefas, que usem os novos métodos das suas novas classes, chamando-as pela classe principal [`Sisreg`](pipelines/datalake/extract_load/sisreg_web_v2/sisreg/sisreg_components/core/sisreg.py).

2. **Atualize o arquivo [`pipelines/datalake/extract_load/sisreg_web_v2/flows.py`](pipelines/datalake/extract_load/sisreg_web_v2/flows.py)**:
   - Garanta que as tarefas que você criou sejam integradas corretamente ao fluxo do Prefect.

3. **Se necessário, atualize o arquivo [`pipelines/datalake/extract_load/sisreg_web_v2/constants.py`](pipelines/datalake/extract_load/sisreg_web_v2/constants.py)**:
    - Evite o *hardcoded*!

Seguindo esses passos você pode estender a funcionalidade do projeto para extrairmos mais dados do SISREG! s2
  
Qualquer dúvida, entre em contato!