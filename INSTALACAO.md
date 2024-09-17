# Instalações


- python 3.11.10
- python venv python3.11
- Airflow 2.9.3
- DBT
- Pyspark


primeiro passo instalar o Python 3.11.10

Instalar venv

Instalar Airflow 

atualizar o flask:
```bash
pip install apache-airflow==2.7.2 flask-appbuilder==4.3.6
```
Instalar o alembic

```bash
pip install alembic==1.13.1
```
Instalar o airflow

```bash
pip install apache-airflow==2.9.3
```

airflow users create \
    --username jezandre \
    --firstname jezandre \
    --lastname jezandre \
    --role Admin \
    --email jezandre_tiago@hotmail.com
