FROM python:3.11-slim

WORKDIR /app

# Install Java (for PySpark) and build dependencies for Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
        build-essential \
        gcc \
        g++ \
        librdkafka-dev \
        libssl-dev \
        libsasl2-dev \
        libffi-dev \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3

# Copy requirements
COPY requirements.txt .

# Upgrade pip and install requirements
RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt
