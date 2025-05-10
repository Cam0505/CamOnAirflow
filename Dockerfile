FROM quay.io/astronomer/astro-runtime:12.9.0-slim

# Copy packages.txt from parent folder into container
COPY packages.txt .

USER root
RUN apt-get update && apt-get install -y git curl build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt/packages.yml dbt/dbt_project.yml ./dbt/
RUN cd dbt && dbt deps

USER astro
# Switch back to Astronomer's default user
WORKDIR /usr/local/airflow

EXPOSE 8080
