### Objetivo das alterações
Generalizar ainda mais o projeto para promover a contribuição facilitada de novos componentes

### O fluxo será sempre:
- task 1: login sisreg (opcional?)

- task 2: extrair_pagina
    - recebe nome de um único método de pagina como _string_

- task 3: encerrar sisreg (opcional?)

- task 4: transform_files
    - adiciona `current_date` no nome do arquivo

- task 4: transform_data
    - transforma _.csv_ pra _parquet_ e faz outras adaptações pro _BQ_

___

### Parâmetros:
- LOGIN (True, False)
    - Define se é necessário fazer login

- CLOSE (True, False)
    - Define se é necessário encerrar a página

- DATASET_ID ("oferta_programada", "execucoes", "afastamentos", etc)
    - Parâmetro passado para a task extrair_pagina saber qual método da classe `Sisreg` executar:
        - `.extrair_oferta_programada()`
        - `.extrair_execucoes()`
        - `.extrair_afastamentos()`
        - etc

    - Parâmetro utilizado para nomear os arquivos _.csv_
        - oferta_programada.csv
        - execucoes.csv
        - etc

    - Parâmetro passado **TAMBÉM** para a task de construção de tabela no _BQ_


___

### Observações:
- Utilizar `case` do _Prefect_ ?
- Importar módulos em tempo de execução usando `importlib`?
- Utilizar `getattr` para pegar o nome do método?
