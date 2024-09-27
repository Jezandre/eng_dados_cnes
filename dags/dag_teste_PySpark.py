from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Definindo o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='pyspark_example_dag',
    default_args=default_args,
    description='DAG de exemplo para executar um job PySpark',
    schedule_interval=None,  # Executa apenas manualmente
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Tarefa para executar um script PySpark
    run_pyspark_job = BashOperator(
        task_id='run_pyspark_job',
        bash_command="""spark-submit --master local[2] /home/jezandre/airflow/dags/dag_teste_PySpark.py"""
    )

    run_pyspark_job
