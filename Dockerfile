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
      firefox-esr \
      chromium \
      chromium-driver \
      curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setting environment with prefect version
ARG PREFECT_VERSION=1.4.1
ENV PREFECT_VERSION="$PREFECT_VERSION"

# Setup virtual environment and prefect
ENV VIRTUAL_ENV="/opt/venv"
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install Python requirements
WORKDIR /app
COPY . .
RUN $VIRTUAL_ENV/bin/pip install --prefer-binary --no-cache-dir -U .

# Ensure npm and npx work properly
RUN npm install -g npm@latest && \
    if ! npm cache clean --force; then echo "Cache clean failed, continuing..."; fi

# Install Puppeteer and Mermaid CLI
RUN npm install puppeteer@23.0.0 @mermaid-js/mermaid-cli@11.2.0

# Install MSSQL dependencies
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install --no-install-recommends -y ffmpeg libsm6 libxext6 msodbcsql17 openssl unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

