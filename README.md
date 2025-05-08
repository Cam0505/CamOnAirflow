CamOnAirflow is a containerized data engineering project that integrates Airflow, DLT (Data Loading Tool), and DBT (Data Build Tool) to automate data pipeline workflows. This project focuses on orchestrating data processes using Apache Airflow while loading and transforming data through DLT and DBT, with the final outputs being stored in a PostgreSQL database.

Features
Airflow Orchestration: Uses Apache Airflow to automate and schedule data pipelines.

DLT Integration: Leverages DLT to efficiently load data from external APIs and sources into a PostgreSQL database.

DBT Models: Implements DBT for data transformation and modeling, allowing for easy analysis and reporting.

Containerization: The project is fully containerized using Docker, ensuring that all dependencies are consistent across environments.

PostgreSQL: Data is stored and processed in a PostgreSQL database for easy querying and reporting.

Technologies Used
Apache Airflow: Orchestrates the workflows and schedules data pipeline tasks.

DLT: Data Loading Tool to load data from external APIs into a PostgreSQL database.

DBT: Data Build Tool to transform and model data for analysis.

PostgreSQL: Relational database for storing and managing the data.

Docker: Containerization for consistent environments across different systems.

