# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from enum import Enum


class informes_seguranca_constants(Enum):

    EMAIL_PATH = "/datarelay"
    EMAIL_URL = "API_URL"
    EMAIL_TOKEN = "API_TOKEN"
    EMAIL_ENDPOINT = "/data/mailman"
    LOGO_DIT_HORIZONTAL_COLORIDO = "https://storage.googleapis.com/sms_dit_arquivos_publicos/img/dit-horizontal-colorido--300px.png"
    LOGO_SMS_HORIZONTAL_COLORIDO = "https://storage.googleapis.com/sms_dit_arquivos_publicos/img/sms-horizontal-gradiente-sem-sus--150px.png"

    # Lista de CIDs desejados, especificados em documento de 39(!) páginas
    # Descrições a partir da lista da OMS
    # [Ref] https://icd.who.int/browse10/2019/en
    CIDS = [
        # W20-W49 Exposure to inanimate mechanical forces
        "W25",  # W25 Contact with sharp glass
        "W26",  # W26 Contact with other sharp objects
        # -- W27-W31
        "W32",  # W32 Handgun discharge
        "W33",  # W33 Rifle, shotgun and larger firearm discharge
        "W34",  # W34 Discharge from other and unspecified firearms
        # -- W35-W43
        "W44",  # W44 Foreign body entering into or through eye or natural orifice
        # W50-W64 Exposure to animate mechanical forces
        "W50",  # W50 Hit, struck, kicked, twisted, bitten or scratched by another person
        "W51",  # W51 Striking against or bumped into by another person
        "W52",  # W52 Crushed, pushed or stepped on by crowd or human stampede
        # X00-X09 Exposure to smoke, fire and flames
        "X00",  # W00 Exposure to smoke, fire and flames
        "X01",  # X01 Exposure to uncontrolled fire, not in building or structure
        "X02",  # X02 Exposure to controlled fire in building or structure
        "X03",  # X03 Exposure to controlled fire, not in building or structure
        "X04",  # X04 Exposure to ignition of highly flammable material
        "X05",  # X05 Exposure to ignition or melting of nightwear
        "X06",  # X06 Exposure to ignition or melting of other clothing and apparel
        "X08",  # X08 Exposure to other specified smoke, fire and flames
        "X09",  # X09 Exposure to unspecified smoke, fire and flames
        # X40-X49 Accidental poisoning by and exposure to noxious substances
        "X4",  # X4x
        # X50-X57 Overexertion, travel and privation
        "X53",  # X53 Lack of food
        "X54",  # X54 Lack of water
        "X57",  # X57 Unspecified privation
        # X58-X59 Accidental exposure to other and unspecified factors
        "X58",  # X58 Exposure to other specified factors
        "X59",  # X59 Exposure to unspecified factor
        # X60-X84 Intentional self-harm
        "X6",  # X6x Intentional self-poisoning by and exposure to (...)
        "X7",  # X7x Intentional self-harm by (...)
        "X8",  # X8x Intentional self-harm by jumping from a high place
        # X85-Y09 Assault
        # ...,  # X8x Assault by (...)
        "X9",  # X90 Assault by (...)
        "Y00",  # Y00 Assault by blunt object
        "Y01",  # Y01 Assault by pushing from high place
        "Y02",  # Y02 Assault by pushing or placing victim before moving object
        # -- Y03
        "Y04",  # Y04 Assault by bodily force
        "Y05",  # Y05 Sexual assault by bodily force
        # Y06 Neglect and abandonment
        "Y060",  # Y06.0 By spouse or partner
        "Y061",  # Y06.1 By parent
        "Y062",  # Y06.2 By acquaintance or friend
        "Y068",  # Y06.8 By other specified persons
        "Y069",  # Y06.9 By unspecified person
        # Y07 Other maltreatment
        "Y070",  # Y07.0 By spouse or partner
        "Y071",  # Y07.1 By parent
        "Y072",  # Y07.2 By acquaintance or friend
        "Y073",  # Y07.3 By official authorities
        "Y078",  # Y07.8 By other specified persons
        "Y079",  # Y07.9 By unspecified person
        "Y08",  # Y08 Assault by other specified means
        "Y09",  # Y09 Assault by unspecified means
        # Y10-Y34 Event of undetermined intent
        "Y1",  # Y1x Poisoning by and exposure to (...)
        "Y2",  # Y2x Hanging; drowning; firearm discharge; explosive; steam; sharp/blunt object
        "Y30",  # Y30 Falling, jumping or pushed from a high place, undetermined intent
        "Y31",  # Y31 Falling, lying or running before or into moving object, undetermined intent
        "Y32",  # Y32 Crashing of motor vehicle, undetermined intent
        "Y33",  # Y33 Other specified events, undetermined intent
        "Y34",  # Y34 Unspecified event, undetermined intent
        # Y35-Y36 Legal intervention and operations of war
        ## Y35 Legal intervention
        "Y350",  # Y35.0 Legal intervention involving firearm discharge
        "Y351",  # Y35.1 Legal intervention involving explosives
        "Y352",  # Y35.2 Legal intervention involving gas
        "Y353",  # Y35.3 Legal intervention involving blunt objects
        "Y354",  # Y35.4 Legal intervention involving sharp objects
        "Y355",  # Y35.5 Legal execution
        "Y356",  # Y35.6 Legal intervention involving other specified means
        "Y357",  # Y35.7 Legal intervention, means unspecified
        ## Y36 Operations of war
        "Y36",  # Y36x
        # Y40-Y84 Complications of medical and surgical care
        ## Y40-Y59 Drugs, medicaments and biological substances causing adverse effects in therapeutic use
        ### Y40 Systemic antibiotics
        "Y400",  # Y40.0 Penicillins
        # Y85-Y89 Sequelae of external causes of morbidity and mortality
        ## Y87 Sequelae of intentional self-harm, assault and events of undetermined intent
        "Y870",  # Y87.0 Sequelae of intentional self-harm
        "Y871",  # Y87.1 Sequelae of assault
        ## Y89 Sequelae of other external causes
        "Y890",  # Y89.0 Sequelae of legal intervention
        "Y891",  # Y89.1 Sequelae of war operations
    ]

    CID_groups = [
        ["W20", "W49", "Exposição a forças mecânicas inanimadas"],
        ["W50", "W64", "Exposição a forças mecânicas animadas"],
        ["X00", "X09", "Exposição à fumaça, ao fogo e às chamas"],
        ["X40", "X49", "Intoxicação acidental por e exposição a substâncias nocivas"],
        ["X50", "X57", "Excesso de esforços, viagens e privações"],
        ["X58", "X59", "Exposição acidental a outros fatores e aos não especificados"],
        ["X60", "X84", "Lesões autoprovocadas intencionalmente"],
        ["X85", "Y09", "Agressões"],
        ["Y10", "Y34", "Eventos cuja intenção é indeterminada"],
        ["Y35", "Y36", "Intervenções legais e operações de guerra"],
        ["Y40", "Y84", "Complicações de assistência médica e cirúrgica"],
        ["Y85", "Y89", "Sequelas de causas externas de morbidade e de mortalidade"],
    ]
