from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ikram',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'aljazeera_pipeline',
    default_args=default_args,
    description='Scraping incrémental Al Jazeera vers Kafka',
    schedule_interval='@hourly',
    start_date=datetime(2026, 5, 8),
    catchup=False,
) as dag:

    scraping_task = BashOperator(
        task_id='run_aljazeera_scraper',
        bash_command='python /opt/airflow/aljazeera.py',
    )