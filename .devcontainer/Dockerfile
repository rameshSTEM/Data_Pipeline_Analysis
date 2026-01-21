FROM python:3.11-slim

WORKDIR /app

# Install Java (Required for PySpark) and procps (Required for Spark monitoring)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Environment variables
# This path is the standard location for the default-jre on Debian
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt