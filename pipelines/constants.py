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
