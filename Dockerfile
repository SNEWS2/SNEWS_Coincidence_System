# Dockerfile for SNEWS_Coincidence_System
FROM python:3.11-bullseye

WORKDIR /app

## Copy the project files
COPY . /app

RUN apt-get update && apt-get install -y --no-install-recommends git build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry

#
## Install dependencies using Poetry
RUN poetry lock
RUN poetry install
RUN poetry run hop auth add hop_creds.csv

CMD ["poetry", "run", "snews_cs", "run-coincidence", "--firedrill"]
#CMD ["tail", "-f", "/dev/null"]
