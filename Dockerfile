FROM mcr.microsoft.com/devcontainers/python:3.11

RUN apt-get update && apt-get install -y git curl build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy only dbt dependency files to leverage Docker cache
COPY dbt/packages.yml dbt/dbt_project.yml ./dbt/

# Run dbt deps to install dbt dependencies
RUN cd dbt && dbt deps

# Copy the rest of your application code
COPY . .

WORKDIR /workspaces/CamOnAirFlow 

USER vscode

