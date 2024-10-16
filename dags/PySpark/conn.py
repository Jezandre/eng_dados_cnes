from pyspark.sql import SparkSession

def connPsql():        
    # Definindo os parâmetros de conexão
    url = "jdbc:postgresql://localhost:5432/airflow_db"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }
    return(url, properties)

