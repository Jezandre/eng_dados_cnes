from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, date_format, lit
from pyspark.sql.types import *
from conn import *


# Inicie a sessão Spark
spark = SparkSession.builder \
    .appName("PostgreSQL to Silver Layer") \
    .config("spark.jars", "/home/jezandre/airflow/postgresql-42.6.0.jar") \
    .getOrCreate()


# Função para mapear esquema PostgreSQL para esquema PySpark
def get_spark_schema_from_postgres(table_name, jdbc_url, properties):
    # Definir o mapeamento dos tipos PostgreSQL para tipos PySpark
    type_mapping = {
        "integer": IntegerType(),
        "bigint": LongType(),
        "smallint": ShortType(),
        "numeric": DoubleType(),
        "decimal": DoubleType(),
        "real": FloatType(),
        "double precision": DoubleType(),
        "varchar": StringType(),
        "char": StringType(),
        "text": StringType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "time": StringType(), 
    }
    # Carregar o DataFrame do PostgreSQL
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
    
    # Obter o esquema PostgreSQL
    postgres_schema = df.dtypes 
    
    # Converter para esquema PySpark
    fields = []
    for col_name, col_type in postgres_schema:
        spark_type = type_mapping.get(col_type.lower(), StringType())  
        fields.append(StructField(col_name, spark_type, True))  
    print(fields)
    return StructType(fields)

# Converte a coluna de data para o formato yyyy-MM-dd
def convert_date_format(column):
    return when(
        column.like("%/%"), to_date(column, "dd/MM/yyyy")
    ).when(
        column.like("%-%"), to_date(column, "dd-MMM-yyyy HH:mm:ss")
    ).otherwise(column)

# Função de tentativa de conversão para tipo numérico
def try_cast_numeric(column):
    return when(col(column).cast("float").isNotNull(), col(column)).otherwise(None)
 




