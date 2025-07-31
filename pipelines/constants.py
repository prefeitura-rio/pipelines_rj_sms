# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
    #######################################
    # Automatically managed,
    # please do not change these values
    #######################################
    # Docker image
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_VERTEX_IMAGE_NAME = "southamerica-east1-docker.pkg.dev/rj-sms/pipelines-rj-sms/image"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    DOCKER_VERTEX_IMAGE = f"{DOCKER_VERTEX_IMAGE_NAME}:{DOCKER_TAG}"
    GCS_FLOWS_BUCKET = "datario-public"
    INFISICAL_ADDRESS = "AUTO_REPLACE_INFISICAL_ADDRESS"
    INFISICAL_TOKEN = "AUTO_REPLACE_INFISICAL_TOKEN"

    ######################################
    # Agent labels
    ######################################
    RJ_SMS_AGENT_LABEL = "sms-new"
    RJ_SMS_AGENT_LABEL__DADOSRIO_CLUSTER = "sms"
    RJ_SMS_VERTEX_AGENT_LABEL = "sms-vertex"

    ######################################
    # Datalake Hub
    ######################################
    DATALAKE_HUB_PATH = "/datalake-hub"
    DATALAKE_HUB_API_URL = "URL"
    DATALAKE_HUB_API_USERNAME = "USERNAME"
    DATALAKE_HUB_API_PASSWORD = "PASSWORD"

    ######################################
    # Other constants
    ######################################
    # EXAMPLE_CONSTANT = "example_constant"

    PROJECT_NAME = {
        "dev": "staging",
        "staging": "staging",
        "prod": "production",
        "local-prod": "production",
        "local-staging": "staging",
    }

    GOOGLE_CLOUD_PROJECT = {
        "dev": "rj-sms-dev",
        "staging": "rj-sms-dev",
        "prod": "rj-sms",
        "local-prod": "rj-sms",
        "local-staging": "rj-sms-dev",
    }

    # DISCORD MENTIONS
    DIT_ID = "&1224334248345862164"

    DANIEL_ID = "1153123302508859422"
    DANILO_ID = "1147152438487416873"
    HERIAN_ID = "213846751247859712"
    KAREN_ID = "722874135893508127"
    MATHEUS_ID = "1184846547242995722"
    AVELLAR_ID = "95182393446502400"
    NATACHA_ID = "1121558659768528916"
    PEDRO_ID = "210481264145334273"
    POLIANA_ID = "1315728001320620064"
    VITORIA_ID = "493160355204431893"
    DAYANE_ID = "316705041161388032"
