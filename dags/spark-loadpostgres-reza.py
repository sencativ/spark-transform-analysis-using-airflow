from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path

default_args = {
    "owner": "Reza",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_load_to_postgres_reza",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Load to Postgres",
    start_date=days_ago(1),
)

Extract = SparkSubmitOperator(
    application="/spark-scripts/spark-extractloadto-postgres-reza.py",
    conn_id="spark_main",
    task_id="spark_submit_task",
    jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",
    dag=spark_dag,
)

Extract
