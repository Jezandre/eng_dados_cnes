# Instalações


- python 3.11.10
- python venv python3.11
- Airflow 2.9.3
- DBT 1.0.0.38.13
- Pyspark


# Instalar o Python 3.11.10

# Instalar venv

# Instalar postgres

# Instalar Airflow

- Atualizar o flask:
ativar
```bash
source airflow_env/bin/activate
```

```bash
pip install apache-airflow==2.7.2 flask-appbuilder==4.3.6
```
- Instalar o alembic

```bash
pip install alembic==1.13.1
```
- Instalar o airflow

```bash
pip install apache-airflow==2.9.3
```
- Criar usuario

airflow users create \
    --username jezandre \
    --firstname jezandre \
    --lastname jezandre \
    --role Admin \
    --email jezandre_tiago@hotmail.com


# Instalar dbt 1.0.0.38.13

- pip install dbt-postgres==1.0.0.38.13
- dbt postgres

# Instalar PySpark

Atualizar instalador e java

```bash
sudo apt update
sudo apt install openjdk-11-jdk
```
baixar nova versão do pyspark

```bash
wget https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz
```
descompactar
```bash
tar xvf spark-3.5.2-bin-hadoop3.tgz
```
mover o arquivo

```bash
sudo mv spark-3.5.2-bin-hadoop3 /opt/spark
```