from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
import os
import shutil
import urllib.request
import zipfile
from dbfread import DBF
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from unidecode import unidecode
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
from requests.exceptions import ReadTimeout
from geopy.extra.rate_limiter import RateLimiter


# Função que verifica se a URL existe
def verifica_url_existe(url, timeout=10):
    try:
        urllib.request.urlopen(url, timeout=timeout)
        return True
    except urllib.error.URLError as e:
        print(f"Erro ao acessar {url}: {e.reason}")
        return False
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return False
    

# Função para testar e encontrar a URL válida
def testarURL():
    data_atual = datetime.now()
    # Definir a pasta de destino
    pasta_destino = r'/home/jezandre/airflow/cnes_zip'

    max_tentativas = 12  # Limita o loop a 12 tentativas (um ano para trás)
    tentativas = 0

    # Iniciando loop para encontrar o arquivo
    while tentativas < max_tentativas:
        ano_atual = data_atual.strftime('%Y')
        mes_atual = data_atual.strftime('%m')
                        
        url = 'ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_' + ano_atual + mes_atual + '.ZIP'
        print(f"Tentando acessar: {url}")
        
        if verifica_url_existe(url):
            print(f"Arquivo encontrado: {url}")
            caminho_zip = f'{pasta_destino}/BASE_DE_DADOS_CNES_{ano_atual}{mes_atual}.ZIP'
            local_filename = f'{pasta_destino}/BASE_DE_DADOS_CNES_{ano_atual}{mes_atual}.ZIP'
            return url, caminho_zip, local_filename
        else:
            # Se o arquivo não for encontrado, subtrai um mês
            data_atual = data_atual - pd.DateOffset(months=1)
            tentativas += 1  # Incrementa o contador de tentativas
            print(f"Tentativa {tentativas}: Não encontrado. Tentando mês anterior...")



def baixarArquivosCSV(**kwargs):
   
    ti = kwargs['ti']
    url, caminho_zip, local_filename = ti.xcom_pull(task_ids='pegar_url')
    pasta_destino = Variable.get("path_file_cnes")
    print("Diretório atual antes do download:", os.getcwd())

    # avaliar se a pasta destino existe, se existir ela será excluída para ser atualizada
    if os.path.exists(pasta_destino):
        shutil.rmtree(pasta_destino)

    print(f'Iniciando download: {datetime.now()}')
    # Baixar o arquivo
    with urllib.request.urlopen(url) as response, open(local_filename, 'wb') as out_file:
        data = response.read()
        out_file.write(data)

    # Criar a pasta de destino se não existir
    print(f'Download finalizado: {datetime.now()}')
    print(f'Iniciando extração: {datetime.now()}')
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    # Extrair o conteúdo do arquivo ZIP
    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
        zip_ref.extractall(pasta_destino)
        
    print("Diretório atual após a extração:", os.getcwd())

    print(f'Arquivos extraídos para: {pasta_destino} - {datetime.now()}')


def renomearArquivos(**kwargs):
    pasta = Variable.get("path_file_cnes")
    print(f'Inciando renomeamento de arquivos: {datetime.now()}')
    padrao_numeros = re.compile(r'\d+$')
    for nome_arquivo in os.listdir(pasta):
        caminho_completo = os.path.join(pasta, nome_arquivo)
        if os.path.isfile(caminho_completo):
            # Extrair o nome do arquivo sem a extensão
            nome_sem_extensao, extensao = os.path.splitext(nome_arquivo)
            # Remover números do final do nome do arquivo
            novo_nome = re.sub(padrao_numeros, '', nome_sem_extensao)
            novo_nome = novo_nome.strip()  # Remover espaços em branco extras
            novo_nome_com_extensao = f"{novo_nome}{extensao}"
            
            # Renomear o arquivo
            novo_caminho = os.path.join(pasta, novo_nome_com_extensao)
            os.rename(caminho_completo, novo_caminho)
            # print(f"Arquivo renomeado: {nome_arquivo} -> {novo_nome_com_extensao}")
    print(f'Finalizando renomeamento de arquivos: {datetime.now()}')


def selecionarArquivosCSVutilizados(**kwargs):
    pasta_destino = Variable.get("path_file_cnes")
    #Dicionário de Variáveis
    csv_files = {
        'tb_estabelecimento': str(pasta_destino) + '/tbEstabelecimento.csv',
        'rl_estab_complementar': str(pasta_destino) + '/rlEstabComplementar.csv',
        'cness_rl_estab_serv_calss': str(pasta_destino) + '/rlEstabServClass.csv',
        'tb_tipo_unidade': str(pasta_destino) + '/tbTipoUnidade.csv',
        'tb_municipio': str(pasta_destino) + '/tbMunicipio.csv',
        'rl_estab_atend_prest_conv': str(pasta_destino) + '/rlEstabAtendPrestConv.csv',
        'tb_estado': str(pasta_destino) + '/tbEstado.csv',
        'tb_servico_especializado': str(pasta_destino) + '/tbServicoEspecializado.csv',
    }

    return csv_files



def criarTabelaCsv(table_name, file_path, **kwargs):
    with open(file_path, 'r', encoding='latin-1') as file:
        columns = file.readline().strip().split(';')
        columns = [col.replace('"', '').strip() for col in columns]    
        columns_str = ', '.join([f'"{col}" VARCHAR' for col in columns])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "bronze"."CNES_{table_name}" ({columns_str});'

        create_table_task = PostgresOperator(
            task_id=f'create_table_{table_name}',
            postgres_conn_id='airflow',
            sql=create_table_sql,
            dag=kwargs['dag'],  # Certifique-se de que a DAG está sendo passada corretamente
        )

    return create_table_task


def criarTabelas(**kwargs):
    csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv')
    
    for table_name, file_path in csv_files.items():
        criarTabelaCsv(table_name, file_path, **kwargs)

def inserirDados(**kwargs):
    
    csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv')
    hook = PostgresHook(postgres_conn_id='airflow')
    # Limpar dados anteriores
    for table_name, file_path in csv_files.items():
        hook.run(f'TRUNCATE TABLE "bronze"."CNES_{table_name}";')  
        hook.run(f'COPY "bronze"."CNES_{table_name}" FROM \'{str(file_path)}\' DELIMITER \';\' CSV HEADER ENCODING \'LATIN1\'')


def rodarQuery(sql_query):
    # Configurações de conexão
    db_url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/airflow_db'

    try:
        # Criando a conexão usando SQLAlchemy
        engine = create_engine(db_url)
        
        # Executando a query e armazenando o resultado em um DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
        
        return df

    except Exception as error:
        print(f"Erro ao executar a query: {error}")
        return None
    
def inserirDadosDf():
    # Construa a URL de conexão (para PostgreSQL, por exemplo)
    db_url = 'postgresql+psycopg2://airflow:airflow@localhost:5432/airflow_db'

    # Crie a engine de conexão
    engine = create_engine(db_url)
    return engine

def adicionarCoordenadas():
    geolocator = Nominatim(user_agent="municipios_ibge", timeout=10)  # Aumenta o tempo limite para 10 segundos
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.0)
    
    # DataFrame
    query = 'SELECT * FROM BRONZE."CNES_tb_municipio";'
    df = rodarQuery(query)
    Municipio='NO_MUNICIPIO'
    Sigla='CO_SIGLA_ESTADO'
    
    total = 0
    latitudes = []
    longitudes = []

    for _, row in df.iterrows():
        try:
            location = geocode(f"{row[Municipio]}, {row[Sigla]}, Brasil")
            if location:
                latitudes.append(location.latitude)
                longitudes.append(location.longitude)
                total += 1
                print(f"Total obtido: {total}")
            else:
                latitudes.append(None)
                longitudes.append(None)
        except (GeocoderUnavailable, ReadTimeout):
            latitudes.append(None)
            longitudes.append(None)
            print(f"Falha ao obter coordenadas para {row[Municipio]}, {row[Sigla]} - Continuando com o próximo item.")

    df['Latitude'] = latitudes
    df['Longitude'] = longitudes
    
    # Inserir no banco dedados
    df.to_sql(
    name='CNES_tb_municipio',
    con=inserirDadosDf(), 
    schema='silver',
    if_exists='append',
    index=False
    )