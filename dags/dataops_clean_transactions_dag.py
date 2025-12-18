from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

default_args = {
    "owner": "dataops",
    "depends_on_past": False,
}

with DAG(
    dag_id="dataops_clean_transactions",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dataops", "ssh", "spark_client"],
) as dag:

    run_cleaning_job = SSHOperator(
        task_id="run_clean_transactions",
        ssh_conn_id="spark_ssh_conn",
        command="""
        set -e
        echo "Downloading CSV from MinIO..."

        wget -O /tmp/dirty_store_transactions.csv \
        http://minio:9000/dataops-bronze/raw/dirty_store_transactions.csv

        echo "Running data cleaning script..."
        python /dataops/clean_transactions.py
        """,
    )

    run_cleaning_job