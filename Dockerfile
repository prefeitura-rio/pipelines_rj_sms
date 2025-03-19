# Build arguments
ARG PYTHON_VERSION=3.10-slim

# Start from the specified Python image
FROM python:${PYTHON_VERSION}

# Set shell options for pipefail
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install apt dependencies and npm
RUN dpkg --add-architecture i386 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    python3-dev \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    firefox-esr \
    chromium \
    chromium-driver \
    curl \
    libstdc++5:i386 \
    libncurses5:i386 \
    xinetd \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setting environment with prefect version
ARG PREFECT_VERSION=1.4.1
ENV PREFECT_VERSION $PREFECT_VERSION

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install Python requirements
WORKDIR /app
COPY . .
RUN $VIRTUAL_ENV/bin/pip install --prefer-binary --no-cache-dir -U .

# Install MSSQL dependencies
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    echo "deb [arch=amd64,arm64,armhf] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install --no-install-recommends -y ffmpeg libsm6 libxext6 msodbcsql17 openssl unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Firebird 1.5.6
WORKDIR /opt
RUN curl -L -o firebird.tar.gz "http://downloads.sourceforge.net/projects/firebird/files/firebird-linux-i386/1.5.6-Release/FirebirdSS-1.5.6.5026-0.nptl.i686.tar.gz" && \
    tar -zxvf firebird.tar.gz && \
    cd FirebirdSS-1.5.6.5026-0.i686 && \
    ./install.sh && \
    rm -rf /opt/firebird.tar.gz /opt/FirebirdSS-1.5.6.5026-0.i686

# Ensure Firebird starts when container runs
CMD ["/etc/init.d/firebird", "start"]
