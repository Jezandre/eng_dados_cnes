from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 19),
    'retries': 1,
}

with DAG(
        'dbt_airflow_example', 
        default_args=default_args, 
        schedule_interval='@daily',
        catchup=False
        ) as dag:
    # Tarefa para executar o comando dbt run

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='source /pa/jezandre/airflow/airflow_env/bin/activate && dbt run --project-dir /home/jezandre/airflow/my_project_psql',
        execution_timeout=timedelta(minutes=2),  # Define o tempo máximo de execução
        dag=dag,
    )
    

    # Definindo a ordem de execução
    dbt_run
