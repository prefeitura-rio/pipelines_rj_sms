# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Vitacare.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitacare flows
    """

    INFISICAL_PATH = "/prontuario-vitacare"
    DATASET_ID = "brutos_prontuario_vitacare"
    BASE_URL = {
        "10": "http://consolidado-ap10.pepvitacare.com:8088",
        "21": "http://consolidado-ap21.pepvitacare.com:8090",
        "22": "http://consolidado-ap22.pepvitacare.com:8091",
        "31": "http://consolidado-ap31.pepvitacare.com:8089",
        "32": "http://consolidado-ap32.pepvitacare.com:8090",
        "33": "http://consolidado-ap33.pepvitacare.com:8089",
        "40": "http://consolidado-ap40.pepvitacare.com:8089",
        "51": "http://consolidado-ap51.pepvitacare.com:8091",
        "52": "http://consolidado-ap52.pepvitacare.com:8088",
        "53": "http://consolidado-ap53.pepvitacare.com:8092",
    }
    ENDPOINT = {
        "posicao": "/reports/pharmacy/stocks",
        "movimento": "/reports/pharmacy/movements",
    }
    CNES = {
        "10": [
            "6023975",  # CF DONA ZICA
            "6028233",  # CF ESTACIO DE SA
            "9057706",  # CF ESTIVADORES
            "2277298",  # CF FERNANDO A BRAGA LOPES
            "9079939",  # CF MEDALHISTA OLIMPICO MAURICIO SILVA
            "9080163",  # CF MEDALHISTA OLIMPICO RICARDO LUCARELLI SOUZA
            "7523246",  # CF NELIO DE OLIVEIRA
            "4030990",  # CF SAO SEBASTIAO
            "6873960",  # CF SERGIO VIEIRA DE MELLO
            "2708426",  # CMS ERNANI AGRICOLA
            "2270250",  # CMS ERNESTO ZEFERINO TIBAU JR
            "2291274",  # CMS JOSE MESSIAS DO CARMO
            "2277301",  # CMS MANOEL ARTHUR VILLABOIM
            "2288346",  # CMS MARCOLINO CANDAU
            "2277328",  # CMS OSWALDO CRUZ
            "2269953",  # CMS SALLES NETTO
            "5621801",  # CSE LAPA
            "6023983",  # CSE SAO FRANCISCO DE ASSIS
        ],
        "21": [
            "6496989",  # CF CANTAGALO PAVAO PAVAOZINHO
            "6503772",  # CF MARIA DO SOCORRO ROCINHA
            "6506232",  # CF RINALDO DE LAMARE
            "6272053",  # CF SANTA MARTA
            "6632831",  # CMS CHAPEU MANG BABILONIA
            "2269651",  # CMS DOM HELDER CAMARA
            "2270072",  # CMS DR ALBERT SABIN
            "2280795",  # CMS JOAO BARROS BARRETO
            "2708434",  # CMS MANOEL JOSE FERREIRA
            "2288370",  # CMS PINDARO DE CARVALHO RODRIGUES
            "7990286",  # CMS ROCHA MAIA
            "2280205",  # CMS RODOLPHO PERISSE VIDIGAL
            "3796310",  # CMS VILA CANOAS
        ],
        "22": [
            "9067078",  # CF ODALEA FIRMO DUTRA
            "3785025",  # CF RECANTO DO TROVADOR
            "2778696",  # CMS CARLOS FIGUEIREDO FILHO BOREL
            "5358612",  # CMS CASA BRANCA
            "2269376",  # CMS HEITOR BELTRAO
            "7414226",  # CMS HELIO PELLEGRINO
            "2280272",  # CMS MARIA AUGUSTA ESTRELLA
            "2280280",  # CMS NICOLA ALBANO
            "2280787",  # CMS NILZA ROSA
        ],
        "31": [
            "5476607",  # CF ADIB JATENE
            "5179726",  # CF ALOYSIO AUGUSTO NOVIS
            "6804209",  # CF ASSIS VALENTE
            "6023320",  # CF AUGUSTO BOAL
            "9345515",  # CF DINIZ BATISTA DOS SANTOS
            "7985657",  # CF EIDIMIR THIAGO DE SOUZA
            "6664075",  # CF FELIPPE CARDOSO
            "6664040",  # CF HEITOR DOS PRAZERES
            "9442251",  # CF JEREMIAS MORAES DA SILVA
            "6932916",  # CF JOAOSINHO TRINTA
            "9075143",  # CF KLEBEL DE OLIVEIRA ROCHA
            "6568491",  # CF MARIA SEBASTIANA DE OLIVEIRA
            "9016805",  # CF NILDA CAMPOS DE LIMA
            "6524486",  # CF RODRIGO Y AGUILAR ROIG
            "9107835",  # CF VALTER FELISBINO DE SOUZA
            "6514022",  # CF VICTOR VALLA
            "9072659",  # CF WILMA COSTA
            "3784975",  # CF ZILDA ARNS
            "2296551",  # CMS AMERICO VELOSO
            "5457009",  # CMS IRACI LOPES
            "3784959",  # CMS JOAO CANDIDO
            "2269902",  # CMS JOSE BREVES DOS SANTOS
            "9391983",  # CMS JOSE PARANHOS FONTENELLE
            "2273640",  # CMS MADRE TERESA DE CALCUTA
            "2295032",  # CMS MARIA CRISTINA ROMA PAUGARTTEN
            "2296535",  # CMS NAGIB JORGE FARAH
            "2280779",  # CMS NECKER PINTO
            "7856954",  # CMS NEWTON ALVES CARDOZO
            "5467136",  # CMS PARQUE ROYAL
            "6664164",  # CMS SAO GODOFREDO
            "5476844",  # CMS VILA DO JOAO
        ],
        "32": [
            "9101764",  # CF AMELIA DOS SANTOS FERREIRA
            "6713564",  # CF ANNA NERY
            "6808077",  # CF ANTHIDIO DIAS DA SILVEIRA
            "6820018",  # CF BARBARA STARFIELD
            "6914152",  # CF BIBI VOGEL
            "7052049",  # CF CARIOCA
            "6762042",  # CF EDNEY CANAZARO DE OLIVEIRA
            "6742130",  # CF EMYGDIO ALVES COSTA FILHO
            "9131795",  # CF ERIVALDO FERNANDES NOBREGA
            "6681379",  # CF HERBERT JOSE DE SOUZA
            "6688152",  # CF IZABEL DOS SANTOS
            "7986505",  # CF LUIZ CELIO PEREIRA
            "9045023",  # CF OLGA PEREIRA PACHECO
            "6919626",  # CF SERGIO NICOLAU AMIN
            "6033121",  # CMS ANTENOR NASCENTES
            "4178602",  # CMS AQUIDABA
            "2273225",  # CMS ARIADNE LOPES DE MENEZES
            "2280744",  # CMS CARLOS GENTILLE DE MELLO
            "2708167",  # CMS CESAR PERNETTA
            "2269503",  # CMS EDUARDO A VILHENA
            "2269805",  # CMS MILTON FONTES MAGARAO
            "2280736",  # CMS RENATO ROCCO
            "6926797",  # CMS RODOLPHO ROCCO
            "5598435",  # CMS TIA ALICE
        ],
        "33": [
            "9057722",  # CF ADERSON FERNANDES
            "7892802",  # CF ADOLFO FERREIRA DE CARVALHO
            "0199338",  # CF ADV MARIO PIRES DA SILVA
            "9128867",  # CF AMAURY BOTTANY
            "6869009",  # CF ANA MARIA CONCEICAO DOS SANTOS CORREIA
            "9111344",  # CF CANDIDO RIBEIRO DA SILVA FILHO
            "7119798",  # CF CARLOS NERY DA COSTA FILHO
            "9078983",  # CF CYPRIANO DAS CHAGAS MEDEIROS
            "7108265",  # CF DANTE ROMANO JUNIOR
            "9131884",  # CF DEPUTADO PEDRO FERNANDES FILHO
            "5044685",  # CF ENFERMEIRA EDMA VALADAO
            "0189200",  # CF ENGENHEIRO SANITARISTA PAULO D AGUILA
            "6793231",  # CF EPITACIO SOARES REIS
            "7998678",  # CF IVANIR DE MELLO
            "6571956",  # CF JOSUETE SANTANNA DE OLIVEIRA
            "6974708",  # CF MAESTRO CELESTINO
            "7088574",  # CF MANOEL FERNANDES DE ARAUJO
            "6029965",  # CF MARCOS VALADAO
            "6761704",  # CF MARIA DE AZEVEDO RODRIGUES PEREIRA
            "9072640",  # CF MESTRE MOLEQUINHO DO IMPERIO
            "7021771",  # CF RAIMUNDO ALVES NASCIMENTO
            "5417708",  # CF SOUZA MARQUES
            "2269937",  # CMS ALBERTO BORGERTH
            "5879655",  # CMS ALICE TOLEDO TIBIRICA
            "2273179",  # CMS AUGUSTO DO AMARAL PEIXOTO
            "2269309",  # CMS CARLOS CRUZ LIMA
            "2269732",  # CMS CARMELA DUTRA
            "2269295",  # CMS CLEMENTINO FRAGA
            "5315026",  # CMS FAZENDA BOTAFOGO
            "2269759",  # CMS FLAVIO DO COUTO VIEIRA
            "2708205",  # CMS MARIO OLINTO DE OLIVEIRA
            "2296586",  # CMS NASCIMENTO GURGEL
            "5315050",  # CMS PORTUS E QUITANDA
            "2269627",  # CMS SYLVIO FREDERICO BRAUNER
        ],
        "40": [
            "9071385",  # CF ARTHUR BISPO DO ROSARIO
            "7892810",  # CF BARBARA MOSLEY DE SOUZA
            "7996675",  # CF GERSON BERGHER
            "7892829",  # CF HELENA BESSERMAN VIANNA
            "7873565",  # CF JOSE DE SOUZA HERDY
            "9127100",  # CF JOSE NEVES
            "0214949",  # CF LOURIVAL FRANCISCO DE OLIVEIRA
            "7995520",  # CF MAICON SIQUEIRA
            "6716598",  # CF MAURY ALVES DE PINHO
            "6927289",  # CF OTTO ALVES DE CARVALHO
            "6927319",  # CF PADRE JOSE DE AZEVEDO TIUBA
            "0265233",  # CF PADRE MARCOS VINICIO MIRANDA VIEIRA
            "2270013",  # CMS CECILIA DONNANGELO
            "4046307",  # CMS HAMILTON LAND
            "2708213",  # CMS HARVEY RIBEIRO DE SOUZA FILHO
            "6784720",  # CMS ITANHANGA
            "2296543",  # CMS JORGE SALDANHA BANDEIRA DE MELLO
            "6927254",  # CMS NEWTON BETHLEM
            "5465877",  # CMS NOVO PALMARES
            "3567508",  # CMS RAPHAEL DE PAULA SOUZA
            "5465885",  # CMS SANTA MARIA
        ],
        "51": [
            "3416321",  # CF ANTONIO GONCALVES DA SILVA
            "3820599",  # CF ARMANDO PALHARES AGUINAGA
            "0193089",  # CF CRISTIANI VIEIRA PINHO
            "7722494",  # CF FAIM PEDRO
            "6023916",  # CF FIORELLO RAYMUNDO
            "6852203",  # CF KELLY CRISTINA DE SA LACERDA SILVA
            "5546591",  # CF MARIA JOSE DE SOUSA BARBOSA
            "6864708",  # CF MARIO DIAS ALENCAR
            "6901042",  # CF NILDO EYMAR DE ALMEIDA AGUIAR
            "6387152",  # CF OLIMPIA ESTEVES
            "6855709",  # CF PADRE JOHN CRIBBIN PADRE JOAO
            "9023089",  # CF ROGERIO PINTO DA MOTA
            "9311661",  # CF ROMULO CARLOS TEIXEIRA
            "3416372",  # CF ROSINO BACCARINI
            "7810172",  # CF SANDRA REGINA SAMPAIO DE SOUZA
            "7874162",  # CF WILSON MELLO SANTOS ZICO
            "2269848",  # CMS ALEXANDER FLEMING
            "2270463",  # CMS ATHAYDE JOSE DA FONSECA
            "3416356",  # CMS BUA BOANERGES BORGES DA FONSECA
            "5546583",  # CMS CATIRI
            "2270579",  # CMS DR EITHEL PINHEIRO DE OLIVEIRA LIMA
            "2270439",  # CMS HENRIQUE MONAT
            "6922031",  # CMS MANOEL GUILHERME DA SILVEIRA FILHO
            "2270560",  # CMS MASAO GOTO
            "2270455",  # CMS PADRE MIGUEL
            "2270552",  # CMS SILVIO BARBOSA
            "2270420",  # CMS WALDYR FRANCO
        ],
        "52": [
            "5620287",  # CF AGENOR DE MIRANDA ARAUJO NETO
            "3567567",  # CF ALKINDAR SOARES PEREIRA FILHO
            "3567540",  # CF ANA GONZAGA
            "7036914",  # CF ANTONIO GONCALVES VILLA SOBRINHO
            "6677711",  # CF DALMIR DE ABREU SALGADO
            "5154197",  # CF DAVID CAPISTRANO FILHO
            "9715444",  # CF DR MYRTES AMORELLI GONZAGA
            "7723296",  # CF EVERTON DE SOUZA SANTOS
            "6648371",  # CF HANS JURGEN FERNANDO DOHMANN
            "7894554",  # CF ISABELA SEVERO DA SILVA
            "6635709",  # CF JOSE DE PAULA LOPES PONTES
            "7908237",  # CF LECY RANQUINE
            "9307265",  # CF MARIA JOSE PAPERA DE AZEVEDO
            "9061401",  # CF MEDALHISTA OLIMPICO ARTHUR ZANETTI
            "9061398",  # CF MEDALHISTA OLIMPICO BRUNO SCHMIDT
            "6029841",  # CF ROGERIO ROCCO
            "7036884",  # CF SONIA MARIA FERREIRA MACHADO
            "3567559",  # CF VALDECIR SALUSTIANO CARDOZO
            "2270323",  # CMS ADAO PEREIRA NUNES
            "6029922",  # CMS AGUIAR TORRES
            "2270277",  # CMS ALVIMAR DE CARVALHO
            "2269554",  # CMS BELIZARIO PENNA
            "6029825",  # CMS CARLOS ALBERTO NASCIMENTO
            "2269562",  # CMS DR MARIO RODRIGUES CID
            "2269546",  # CMS DR OSWALDO VILELLA
            "2269538",  # CMS EDGARD MAGALHAES GOMES
            "2269511",  # CMS GARFIELD DE ALMEIDA
            "2270315",  # CMS MAIA BITTENCOURT
            "2270366",  # CMS MANOEL DE ABREU
            "2270633",  # CMS MARIO VITOR DE ASSIS PACHECO
            "2270307",  # CMS MOURAO FILHO
            "2270641",  # CMS PEDRO NAVA
            "2270293",  # CMS RAUL BARROSO
            "5670357",  # CMS VILA DO CEU
            "2270285",  # CMS WOODROW PIMENTEL PANTOJA
        ],
        "53": [
            "7896204",  # CF ALICE DE JESUS REGO
            "6660185",  # CF DEOLINDO COUTO
            "6671020",  # CF EDSON ABDALLA SAAD
            "2280310",  # CF ERNANI DE PAIVA FERREIRA BRAGA
            "6618863",  # CF HELANDE DE MELLO GONCALVES
            "6559727",  # CF ILZO MOTTA DE MELLO
            "6618871",  # CF JAMIL HADDAD
            "2295237",  # CF JOAO BATISTA CHAGAS
            "6581994",  # CF JOSE ANTONIO CIRAUDO
            "6559735",  # CF LENICE MARIA MONTEIRO COELHO
            "6572014",  # CF LOURENCO DE MELLO
            "6683851",  # CF SAMUEL PENHA VALLE
            "6618855",  # CF SERGIO AROUCA
            "3785009",  # CF VALERIA GOMES ESTEVES
            "2295253",  # CF WALDEMAR BERARDINELLI
            "6026737",  # CMS ADELINO SIMOES NOVA SEPETIBA
            "2273551",  # CMS ALOYSIO AMANCIO DA SILVA
            "2269929",  # CMS CATTAPRETA
            "2273578",  # CMS CESARIO DE MELLO
            "2273616",  # CMS CYRO DE MELLO MANGUARIBA
            "2708183",  # CMS DECIO AMARAL FILHO
            "2273586",  # CMS EMYDIO CABRAL
            "2273543",  # CMS FLORIPES GALDINO PEREIRA
            "2280760",  # CMS MARIA APARECIDA DE ALMEIDA
            "2806320",  # CMS SAVIO ANTUNES ANTARES
        ],
    }
