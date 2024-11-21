from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, regexp_replace, date_format, lit
from pyspark.sql.types import StringType
from conn import *
from pyspark_functions import *



# Configuração da Spark Session
spark = SparkSession.builder \
    .appName("OptimizeQueryAndInsertToPostgres") \
    .config("spark.jars", "/home/jezandre/airflow/postgresql-42.6.0.jar") \
    .getOrCreate()

def estabelecimentoEnriquecimento():    
    # Configurações para o banco PostgreSQL
    jdbc_url, jdbc_properties = connPsql()

    tabela_name ='"CNES_tb_estabelecimento"'

    # Carregar os dados da tabela CNES_tb_estabelecimento e CNES_tb_municipio (nesta parte, adaptando ao seu contexto de carga de dados)
    estabelecimento_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f'bronze.{tabela_name}') \
        .options(**jdbc_properties) \
        .load()

    municipio_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", 'silver."CNES_tb_municipio"') \
        .options(**jdbc_properties) \
        .load()

    # Aplicando a função nas colunas de data
    estabelecimento_df = estabelecimento_df.withColumn(
        "DT_EXPEDICAO",
        convert_date_format(
            estabelecimento_df["DT_EXPEDICAO"]
            )
        )
    estabelecimento_df = estabelecimento_df.withColumn(    
        "DT_ATUALIZACAO_ORIGEM",
        convert_date_format(
            estabelecimento_df["TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')"]
            )
        )
    # Join entre as tabelas
    resultado_df = estabelecimento_df.alias("est").join(
        municipio_df.alias("mun"),
        col("est.CO_MUNICIPIO_GESTOR") == col("mun.CO_MUNICIPIO"),
        "left"
    ).select(
        "CO_UNIDADE"
        , "CO_CNES"
        , "NO_RAZAO_SOCIAL"
        , "NO_FANTASIA"
        , "CO_REGIAO_SAUDE"
        , "CO_MICRO_REGIAO"
        , "CO_DISTRITO_SANITARIO"
        , "CO_ATIVIDADE"
        , "CO_CLIENTELA"
        , "DT_EXPEDICAO"
        , "TP_LIC_SANI"
        , "CO_TURNO_ATENDIMENTO"
        , "CO_ESTADO_GESTOR"
        , "CO_MUNICIPIO_GESTOR"
        , "CO_MOTIVO_DESAB"
        , "CO_TIPO_UNIDADE"
        , "TP_GESTAO"
        , "CO_TIPO_ESTABELECIMENTO"
        , "CO_ATIVIDADE_PRINCIPAL"
        , "ST_CONTRATO_FORMALIZADO"
        , "CO_TIPO_ABRANGENCIA"
        , "ST_COWORKING"
        , when(try_cast_numeric("NU_LATITUDE").isNull(), col("mun.Latitude").cast("string"))
            .otherwise(col("est.NU_LATITUDE")).alias("NU_LATITUDE")
        , when(try_cast_numeric("NU_LONGITUDE").isNull(), col("mun.Longitude").cast("string"))
            .otherwise(col("est.NU_LONGITUDE")).alias("NU_LONGITUDE")
        , "DT_ATUALIZACAO_ORIGEM"
    )

    # Exibir resultado
    resultado_df.show()
    # Escrever o DataFrame otimizado no PostgreSQL
    resultado_df.write \
        .jdbc(url=jdbc_url, table=f'silver.{tabela_name}', mode="overwrite", properties=jdbc_properties)

    spark.stop()
    
    return print('Tabela enriquecida com sucesso')