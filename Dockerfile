# Dockerfile for SNEWS_Coincidence_System
FROM python:3.11-bullseye

WORKDIR /app

## Copy the project files
COPY . /app

RUN apt-get update && apt-get install -y --no-install-recommends git build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Accept build arguments
ARG HOP_USERNAME
ARG HOP_PASSWORD

# Set environment variables
ENV HOP_USERNAME=${HOP_USERNAME}
ENV HOP_PASSWORD=${HOP_PASSWORD}

# Get credentials into container
RUN chmod +x /app/generate_firedrill_creds.sh
RUN /app/generate_firedrill_creds.sh

RUN sed -i 's/^FIREDRILL_OBSERVATION_TOPIC=.*/FIREDRILL_OBSERVATION_TOPIC=kafka:\/\/$\{HOP_BROKER\}\/snews\.experiments-github/' /app/snews_cs/etc/test-config.env
RUN sed -i 's/^FIREDRILL_ALERT_TOPIC=.*/FIREDRILL_ALERT_TOPIC=kafka:\/\/$\{HOP_BROKER\}\/snews\.alert-github/' /app/snews_cs/etc/test-config.env

# Install Poetry
RUN pip install --no-cache-dir poetry

## Install dependencies using Poetry
RUN poetry lock
RUN poetry install
RUN poetry run hop auth add hop_creds.csv

CMD ["poetry", "run", "snews_cs", "run-coincidence", "--firedrill"]
