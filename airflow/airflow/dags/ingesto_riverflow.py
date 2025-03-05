from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Definição dos argumentos padrão da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Criando a DAG
dag = DAG(
    "riverflow_pipeline",
    default_args=default_args,
    description="Pipeline de ingestão e transformação do Riverflow",
    schedule_interval=timedelta(days=1),  # Rodando diariamente
    catchup=False,
)

# Função para rodar a ingestão
def run_ingestion():
    subprocess.run(["python", "/opt/riverflow/ingestion/ingestion.py"], check=True)

# Função para rodar o dbt
def run_dbt():
    subprocess.run(["dbt", "run"], cwd="/opt/riverflow/dbt", check=True)

# Task para rodar a ingestão
ingestion_task = PythonOperator(
    task_id="run_ingestion",
    python_callable=run_ingestion,
    dag=dag,
)

# Task para rodar o dbt
dbt_task = PythonOperator(
    task_id="run_dbt",
    python_callable=run_dbt,
    dag=dag,
)

# Definir a ordem de execução das tasks
ingestion_task >> dbt_task
