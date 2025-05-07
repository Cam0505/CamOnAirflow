FROM quay.io/astronomer/astro-runtime:12.7.1

WORKDIR "/usr/local/airflow"

# Switch to root user to add the airflow user and group
USER root

# Create the airflow user and group
RUN groupadd -r airflow && useradd -r -g airflow airflow

# Create /home/airflow directory if it doesn't exist, then give proper permissions
RUN mkdir -p /home/airflow && chown -R airflow: /home/airflow

# Create log directory with proper permissions for airflow user
RUN mkdir -p /usr/local/airflow/dbt/logs && \
    chown -R airflow:airflow /usr/local/airflow/dbt/logs

RUN mkdir -p /usr/local/airflow/dbt/target/compiled && \
    chown -R airflow:airflow /usr/local/airflow/dbt/target/compiled

# Upgrade pip to the latest version with --user to avoid permission issues
RUN pip install --upgrade --user pip

# Switch back to the airflow user after setup
USER airflow

# Ensure /tmp directory is writable by airflow
RUN mkdir -p /tmp && \
    touch /tmp/dbt.log && \
    chown airflow: /tmp/dbt.log

# Copy requirements and install Python dependencies
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

RUN pipdeptree