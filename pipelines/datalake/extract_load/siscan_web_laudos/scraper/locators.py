# -*- coding: utf-8 -*-
"""Coleção de localizadores (By, value) em português."""

from selenium.webdriver.common.by import By

# Autenticação
CAMPO_EMAIL = (By.NAME, "email")
CAMPO_SENHA = (By.NAME, "senha")
BOTAO_ENTRAR = (By.NAME, "j_id34")

# Menu
MENU_EXAME = (By.ID, "j_id32:j_id66_span")
MENU_GERENCIAR_LAUDO = (By.ID, "j_id32:j_id68")

# Pesquisa
OPCAO_EXAME_MAMO = (By.ID, "form:tpExame:4")
OPCAO_MUNICIPIO = (By.ID, "form:j_id120:1")
OPCAO_FILTRO_DATA = (By.ID, "form:porData:0")
CAMPO_DATA_INICIO = (By.ID, "form:dataInicioInputDate")
CAMPO_DATA_FIM = (By.ID, "form:dataFimInputDate")
BOTAO_PESQUISAR = (By.ID, "form:botaoPesquisarLaudo")

# Tabela
LUPA_LAUDO = (By.CSS_SELECTOR, "a[title='Detalhar Laudo']")
BOTAO_PROXIMO = (
    By.XPATH,
    "//td[contains(@class,'rich-datascr')][span[normalize-space()='Próximo']]",
)

# Detalhes ----

# Unidade de saude
DET_NOME = (By.ID, "form:nome")
DET_UF = (By.ID, "form:UF")
DET_NEXAME = (By.ID, "form:NExame")
DET_DATA = (By.ID, "form:DataColeta")
DET_CNES = (By.ID, "form:CNES")
DET_MUNICIPIO = (By.ID, "form:Municipio")
DET_NPRONT = (By.ID, "form:Nprontuario")
DET_NPROTO = (By.ID, "form:Nprotocolo")

# PACIENTE
DET_CARTAO_SUS = (By.ID, "form:CartaoSUS")
DET_NOME_PACIENTE = (By.ID, "form:NomePaciente")
DET_DATA_NASCIMENTO = (By.ID, "form:DataNascimento")
DET_MAE = (By.ID, "form:Mae")
DET_UF_PACIENTE = (By.ID, "form:UFPaciente")
DET_BAIRRO = (By.ID, "form:Bairro")
DET_NUMERO = (By.ID, "form:Numero")
DET_CEP = (By.ID, "form:cep2")
DET_SEXO = (By.ID, "form:Sexo")
DET_IDADE = (By.ID, "form:Idade")
DET_TELEFONE = (By.ID, "form:Telefone")
DET_MUNICIPIO_PACIENTE = (By.ID, "form:MunicipioPaciente")
DET_ENDERECO = (By.ID, "form:Endereco")
DET_COMPLEMENTO = (By.ID, "form:Complemento")

# PRESTADOR
DET_NOME_PRESTADOR = (By.ID, "form:NomePrestador")
DET_CNPJ = (By.ID, "form:CNPJ")
DET_UF_PRESTADOR = (By.ID, "form:UFPrestador")
DET_CNES_PRESTADOR = (By.ID, "form:CNESPrestador")
DET_DATA_RECEBIMENTO = (By.ID, "form:DataRecebimento")
DET_MUNICIPIO_PRESTADOR = (By.ID, "form:MunicipioPrestador")

# INDICAÇÃO
DET_TIPO_MAMOGRAFIA = (By.ID, "form:tipoMamografia")
DET_TIPO_MAMOGRAFIA_RASTREAMENTO = (By.ID, "form:tipoMamografiaRastreamento")
DET_ACHADO_EXAME_CLINICO = (By.ID, "form:j_id121")
DET_ACHADO_EXAME_DIREITA = (By.ID, "form:achadoExameDireita")
DET_DATA_ULTIMA_MENSTRUACAO = (By.ID, "form:j_id149")

# MAMOGRAFIA
DET_NUMERO_FILMES = (By.ID, "form:numeroFilmes")
DET_MAMA_DIREITA_PELE = (By.ID, "form:mamaDireitaPele")
DET_TIPO_MAMA_DIREITA = (By.ID, "form:tipoMamaDireita")
DET_MICROCALCIFICACOES = (By.ID, "form:j_id186")
DET_LINFONODOS_AXILIARES_DIREITA = (By.ID, "form:linfonodosAxiliaresDireita")
DET_ACHADOS_BENIGNOS_DIREITA = (By.ID, "form:achadosBenignosOpcoesDireita")
DET_MAMA_ESQUERDA_PELE = (By.ID, "form:mamaEsquerdaPele")
DET_TIPO_MAMA_ESQUERDA = (By.ID, "form:tipoMamaEsquerda")
DET_LINFONODOS_AXILIARES_ESQUERDA = (By.ID, "form:linfonodosAxiliaresEsquerda")
DET_ACHADOS_BENIGNOS_ESQUERDA = (By.ID, "form:achadosBenignosOpcoesEsquerda")

# CLASSIFICAÇÃO RADIOLÓGICA
DET_CLASSIF_RADIOLOGICA_DIREITA = (By.ID, "form:j_id248")
DET_CLASSIF_RADIOLOGICA_ESQUERDA = (By.ID, "form:j_id252")
DET_MAMAS_LABELS = (By.CLASS_NAME, "form-lbl")  # contém os dois textos das mamas

# RECOMENDAÇÕES
DET_RECOMENDACOES = (By.ID, "form:j_id259")

# OBSERVAÇÕES GERAIS
DET_OBSERVACOES_GERAIS = (By.XPATH, "//*[@title='Observações Gerais']")

# RESPONSÁVEL PELO RESULTADO
DET_RESPONSAVEL_RESULTADO = (By.ID, "form:responsavelResultado")
DET_CNS_RESULTADO = (By.ID, "form:cnsResultado")
DET_CONSELHO = (By.ID, "form:conselho")
DET_DATA_LIBERACAO_RESULTADO = (By.ID, "form:dataLiberacaoResultado")

# Botão
BOTAO_VOLTAR = (By.NAME, "form:j_id285")
