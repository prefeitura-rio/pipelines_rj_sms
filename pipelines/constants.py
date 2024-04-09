# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
    ######################################
    # Automatically managed,
    # please do not change these values
    ######################################
    # Docker image
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_VERTEX_IMAGE_NAME = "southamerica-east1-docker.pkg.dev/rj-sms/pipelines-rj-sms/image"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    DOCKER_VERTEX_IMAGE = f"{DOCKER_VERTEX_IMAGE_NAME}:{DOCKER_TAG}"
    GCS_FLOWS_BUCKET = "datario-public"

    ######################################
    # Agent labels
    ######################################
    RJ_SMS_AGENT_LABEL = "sms"

    ######################################
    # Other constants
    ######################################
    # EXAMPLE_CONSTANT = "example_constant"

    PROJECT_NAME = {
        "dev": "staging",
        "prod": "production",
    }
