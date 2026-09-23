FROM apache/airflow:2.11.2-python3.12

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.11.0
