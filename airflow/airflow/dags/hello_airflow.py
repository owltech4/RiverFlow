from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Função de teste
def print_hello():
    print("Hello, Airflow!")

# Definição da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),  # Adjust for current date
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hello_airflow",
    default_args=default_args,
    description="A simple DAG that prints Hello, Airflow!",
    schedule_interval="*/5 * * * *",  # Each 5 minutes
    catchup=False,
) as dag:

    task_hello = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    task_hello

