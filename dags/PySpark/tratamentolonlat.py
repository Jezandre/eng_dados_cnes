from conn import connPsql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace

# Cria uma sessão Spark e adiciona o driver JDBC ao classpath
spark = SparkSession.builder \
    .appName("Análise de Longitude e Latitude") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  

url, properties = connPsql()

tabela = 'BRONZE."CNES_tb_estabelecimento"'

# Carregando dados do PostgreSQL para um DataFrame do Spark
df = spark.read.jdbc(url=url, table= 'BRONZE."CNES_tb_estabelecimento"', properties=properties)

# Nome das colunas de longitude e latitude
longitude_col = "NU_LONGITUDE"  # Substitua pelo nome real da coluna
latitude_col = "NU_LATITUDE"  # Substitua pelo nome real da coluna

total_linhas = df.count()

# Função para analisar colunas e retornar resultados
def analisar_coluna(df, coluna):
    resultados = {}

    # DataFrames para valores problemáticos
    df_dados_vazios = df.filter(col(coluna).isNull() | (col(coluna) == ""))
    df_dados_nao_numericos = df.filter(~col(coluna).cast("double").isNotNull())
    df_valores_com_virgula = df.filter(col(coluna).contains(","))
    df_valores_invalidos = df.filter((col(coluna) < -180) | (col(coluna) > 180))  # Para longitude

    # Armazena as contagens
    resultados['dados_vazios'] = df_dados_vazios.count()
    resultados['dados_nao_numericos'] = df_dados_nao_numericos.count()
    resultados['valores_com_virgula'] = df_valores_com_virgula.count()
    resultados['valores_invalidos'] = df_valores_invalidos.count()

    return resultados, df_dados_vazios, df_dados_nao_numericos, df_valores_com_virgula, df_valores_invalidos

# Função para imprimir DataFrames problemáticos
def imprimir_dados_problematicos(df_dados_vazios, df_dados_nao_numericos, df_valores_com_virgula, df_valores_invalidos, coluna):
    print(f"\nDataFrame com Dados Vazios ({coluna}):")
    df_dados_vazios.select(coluna).show(truncate=False)

    print(f"\nDataFrame com Dados Não Numéricos ({coluna}):")
    df_dados_nao_numericos.select(coluna).show(truncate=False)

    print(f"\nDataFrame com Valores com Vírgula ({coluna}):")
    df_valores_com_virgula.select(coluna).show(truncate=False)

    print(f"\nDataFrame com Valores Inválidos ({coluna}):")
    df_valores_invalidos.select(coluna).show(truncate=False)

# # Analisando as colunas de longitude e latitude
# resultados_longitude, df_dados_vazios_long, df_dados_nao_numericos_long, df_valores_com_virgula_long, df_valores_invalidos_long = analisar_coluna(df, longitude_col)
# resultados_latitude, df_dados_vazios_lat, df_dados_nao_numericos_lat, df_valores_com_virgula_lat, df_valores_invalidos_lat = analisar_coluna(df, latitude_col)

# # Mostrando os resultados
# print("Resultados para Longitude:")
# for key, value in resultados_longitude.items():
#     print(f"{key}: {value}")

# # Imprimindo DataFrames problemáticos para longitude
# imprimir_dados_problematicos(df_dados_vazios_long, df_dados_nao_numericos_long, df_valores_com_virgula_long, df_valores_invalidos_long, longitude_col)

# print("Resultados para Latitude:")
# for key, value in resultados_latitude.items():
#     print(f"{key}: {value}")

# # Imprimindo DataFrames problemáticos para latitude
# imprimir_dados_problematicos(df_dados_vazios_lat, df_dados_nao_numericos_lat, df_valores_com_virgula_lat, df_valores_invalidos_lat, latitude_col)

# TRATANDO VALORES COM VIRGULA

def tratar_valores_com_virgula(df, coluna):
    # Substitui vírgulas por pontos na coluna especificada
    df_tratado = df.withColumn(
        coluna,
        regexp_replace(col(coluna), ',', '.')
    )
    df_tratado = df_tratado.withColumn(
        coluna,
        when(col(coluna) == "", None).otherwise(col(coluna))
    )
    return df_tratado

# Tratando valores com vírgula nas colunas de longitude e latitude
df_longitude_tratado = tratar_valores_com_virgula(df, longitude_col)
df_latitude_tratado = tratar_valores_com_virgula(df, latitude_col)


# Analisando as colunas de longitude e latitude
resultados_longitude, df_dados_vazios_long, df_dados_nao_numericos_long, df_valores_com_virgula_long, df_valores_invalidos_long = analisar_coluna(df_longitude_tratado, longitude_col)
resultados_latitude, df_dados_vazios_lat, df_dados_nao_numericos_lat, df_valores_com_virgula_lat, df_valores_invalidos_lat = analisar_coluna(df_latitude_tratado, latitude_col)

# Mostrando os resultados
print("Resultados para Longitude:")
for key, value in resultados_longitude.items():
    print(f"{key}: {value}")

# Imprimindo DataFrames problemáticos para longitude
imprimir_dados_problematicos(df_dados_vazios_long, df_dados_nao_numericos_long, df_valores_com_virgula_long, df_valores_invalidos_long, longitude_col)

print("Resultados para Latitude:")
for key, value in resultados_latitude.items():
    print(f"{key}: {value}")

# Imprimindo DataFrames problemáticos para latitude
imprimir_dados_problematicos(df_dados_vazios_lat, df_dados_nao_numericos_lat, df_valores_com_virgula_lat, df_valores_invalidos_lat, latitude_col)

df_fora_brasil = df_longitude_tratado.filter(
    (col(latitude_col) < -34) | (col(latitude_col) > 5) |  # Faixa de Latitude
    (col(longitude_col) < -73) | (col(longitude_col) > -34)  # Faixa de Longitude
)

# Mostrar resultados
df_pandas = df_fora_brasil.toPandas()
print(df_pandas[[latitude_col,longitude_col]])


# Finaliza a sessão Spark
spark.stop()


