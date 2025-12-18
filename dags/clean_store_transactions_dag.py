from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

default_args = {
    "owner": "burcu",
    "start_date": datetime(2025, 12, 18),
}

with DAG(
    dag_id="clean_store_transactions_pipeline",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["dataops", "ssh", "spark"],
) as dag:

    run_cleaning_job = SSHOperator(
        task_id="run_cleaning_on_spark_client",
        ssh_conn_id="spark_ssh_conn",
        command="""
        wget -O /tmp/dirty_store_transactions.csv \
        https://raw.githubusercontent.com/erkansirin78/datasets/master/dirty_store_transactions.csv && \
        python /opt/airflow/dags/../apps/clean_store_transactions.py
        """
    )

    run_cleaning_job
