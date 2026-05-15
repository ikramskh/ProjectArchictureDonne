from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "anass",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    "medallion_gold_layer",
    default_args=default_args,
    description="Pipeline Big Data News Analytics",
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False
)

gold_task = BashOperator(
    task_id="gold_processing",
    bash_command="""
    spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 512M \
    --executor-cores 1 \
    --total-executor-cores 1 \
    --jars /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/postgresql-42.7.3.jar \
    /opt/spark_jobs/gold_processor.py
    """,
    dag=dag
)
