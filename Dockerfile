FROM mcr.microsoft.com/devcontainers/python:3.11

USER root
RUN apt-get update && apt-get install -y git curl build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt/packages.yml dbt/dbt_project.yml ./dbt/
RUN cd dbt && dbt deps

USER vscode
WORKDIR /workspaces/CamOnAirFlow 

EXPOSE 8080
