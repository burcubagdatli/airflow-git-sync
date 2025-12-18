from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    dag_id="ssh_connection_test",
    start_date=datetime(2025, 12, 18),
    schedule="@once",
    catchup=False,
) as dag:
    SSHOperator(
        task_id="test_spark_ssh",
        ssh_conn_id="spark_ssh_conn",
        command="whoami && hostname && python --version",
    )
