from pyspark.sql import SparkSession
from pyspark.sql.types import *
from conn import *


# Inicie a sessão Spark
spark = SparkSession.builder \
    .appName("PostgreSQL to Silver Layer") \
    .config("spark.jars", "/home/jezandre/airflow/postgresql-42.6.0.jar") \
    .getOrCreate()


tables = [
        '"CNES_tb_estabelecimento"',
        '"CNES_rl_estab_complementar"',
        '"CNES_cness_rl_estab_serv_calss"',
        '"CNES_tb_tipo_unidade"',
        '"CNES_tb_municipio"',
        '"CNES_rl_estab_atend_prest_conv"',
        '"CNES_tb_estado"',
        '"CNES_tb_servico_especializado"'
        ]

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

# Configurações de conexão
jdbc_url, properties = connPsql()

# Definir nome da tabela no PostgreSQL
for table_name in tables:
    # Obter o esquema convertido
    table_bronze = f'BRONZE.{table_name}'
    spark_schema = get_spark_schema_from_postgres(table_bronze, jdbc_url, properties)

    # Ler dados da camada bronze com o novo esquema
    df = spark.read.jdbc(url=jdbc_url, table=table_bronze, properties=properties)
    
    # Defina o nome da tabela de destino na camada silver
    table_silver = f'SILVER.{table_name}'
    
    # Processar e salvar na camada silver do banco de dados
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_silver) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

 




