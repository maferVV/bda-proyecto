# Use the official Spark image
FROM bitnami/spark:latest

USER root

# Set the working directory
WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy your ETL scripts into the container
COPY ./scripts /app/scripts

COPY ./requirements.txt .
# Check if requirements.txt exists and list its contents
RUN ls -l /app && cat requirements.txt
# Upgrade pip and install dependencies
RUN pip install --upgrade pip #&& pip install --no-cache-dir -r --resume-retries requirements.txt
