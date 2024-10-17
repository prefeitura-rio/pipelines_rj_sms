# Build arguments
ARG PYTHON_VERSION=3.10-slim

# Start from the specified Python image
FROM python:${PYTHON_VERSION}

# Set shell options for pipefail
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install apt dependencies and npm
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    chromium \
    chromium-driver \
    curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setting environment with prefect version
ARG PREFECT_VERSION=1.4.1
ENV PREFECT_VERSION=$PREFECT_VERSION

# Setup virtual environment and install Prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install Python requirements
WORKDIR /app
COPY . .
RUN $VIRTUAL_ENV/bin/pip install --prefer-binary --no-cache-dir -U .

# Ensure npm and npx work properly
RUN npm install -g npm@latest && \
    if ! npm cache clean --force; then echo "Cache clean failed, continuing..."; fi

# Install Puppeteer and Mermaid CLI
RUN npm install puppeteer@23.0.0 @mermaid-js/mermaid-cli@11.2.0
