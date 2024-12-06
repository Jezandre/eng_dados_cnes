from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import json
import os
import urllib.request
from datetime import datetime
import zipfile
import pandas as pd
import pyodbc
import os
import re
from sqlalchemy import create_engine, text 
import shutil
from urllib.error import URLError
import configparser
from Python_Functions.utils import *

default_args={
    'depends_on_past': False,
    # 'email': ['<<Coloque o email de notificação aqui>>'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'catchup': False
}

dag = DAG(
    'cnes_files',
    description='Dados do dataSUS para download',
    schedule_interval=None,
    start_date=datetime(2023,3,5),
    catchup=False,
    default_args=default_args,
    default_view='graph',
    doc_md='DAG para registrar dados de unidades de saúde'
    )


pegar_url_task = PythonOperator(
    task_id = 'pegar_url',
    python_callable = testarURL,
    provide_context = True,
    dag=dag
)


baixar_arquivos_task = PythonOperator(
    task_id='baixar_arquivos',
    python_callable=baixarArquivosCSV,    
    provide_context=True,
    dag=dag
)

renomear_arquivos_task = PythonOperator(
    task_id='renomear_arquivos',
    python_callable=renomearArquivos,
    provide_context=True,
    dag=dag
)



selecionar_arquivos_task = PythonOperator(
    task_id='obter_arquivos_csv',
    python_callable=selecionarArquivosCSVutilizados,
    provide_context=True,
    dag=dag,
)


criar_tabelas_task = PythonOperator(
    task_id='criar_tabelas_from_csv',
    python_callable=criarTabelas,
    dag=dag
)


inserir_dados_task = PythonOperator(
    task_id='inserir_dados',
    python_callable=inserirDados,
    provide_context=True,
    dag=dag
)

add_coordenadas = PythonOperator(
    task_id='adicionar_coordenadas',
    python_callable=adicionarCoordenadas,
    provide_context=True,
    dag=dag
)

run_pyspark_job = BashOperator(
    task_id='run_pyspark_job',
    bash_command="""spark-submit --jars /home/jezandre/airflow/postgresql-42.6.0.jar /home/jezandre/airflow/dags/PySpark/main.py""",
    dag=dag
)


pegar_url_task >> baixar_arquivos_task >> renomear_arquivos_task >> selecionar_arquivos_task >> criar_tabelas_task >> inserir_dados_task >> add_coordenadas >> run_pyspark_job