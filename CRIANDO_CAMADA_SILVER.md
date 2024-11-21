# Criando a camada silver

A camada Silver é a etapa em que precisamos filtrar e limpar os dados para garantir uma análise precisa e confiável. Nessa fase, trataremos dados duplicados, inválidos, nulos, entre outros possíveis problemas. Neste caso, optei por utilizar o PySpark para essa tarefa, com o objetivo principal de demonstrar como essa ferramenta funciona em um contexto prático. Sei que os mais experientes podem questionar essa escolha, pois não estamos lidando com milhões de registros, mas acredito que é uma abordagem interessante para explorar o potencial do PySpark em uma base significativa de dados.

Portanto, para o tratamento das tabelas, utilizaremos o PySpark.

Nos dados do CNES, uma das análises que pretendo explorar está relacionada à localização dos estabelecimentos de saúde. Com esses dados, podemos obter insights valiosos sobre a distribuição geográfica desses estabelecimentos. Para isso, precisaremos analisar cuidadosamente as colunas e tabelas que trazem essas informações de localização e ver como estão estruturadas.

# Enriquecendo a tabela municipios com dados de Latitude e longitude

Explorando os dados que temos na principal tabela de estabelecimentos de saúde, observei que as colunas de latitude e longitude trazem informações muito interessantes, que podem nos ajudar a criar mapas e identificar as principais localizações de alguns estabelecimentos de saúde. No entanto, ao analisar esses dados, identifiquei que cerca de 60 mil linhas não possuem essas informações, o que representa um total de aproximadamente 10% de toda a base.

Diante disso, pensei em buscar as informações de latitude e longitude da cidade onde o estabelecimento está localizado. Essa abordagem reduziria a precisão da localização, mas, ao menos, teríamos o estabelecimento posicionado em um local próximo ao real.

Portanto, precisaremos enriquecer a nossa base com algumas dessas informações. Latitude e longitude são dados geográficos, então a primeira ideia que tive foi buscar esses dados, seja em algum site do IBGE ou até mesmo por meio de uma API, o que poderia ser mais eficiente. No entanto, me deparei com um problema: embora os municípios no IBGE possuam um código identificador, na base do CNES a tabela de municípios não possui essa correspondência. Isso representa um desafio adicional para conectar esses dados e obter a localização correta, uma vez que existem municípios com o mesmo nome e, além disso, alguns municípios no CNES têm grafias diferentes das do IBGE. Para resolver isso, usei uma biblioteca que trata e identifica nomes com grafias próximas.

Realizei uma pesquisa no Google para encontrar esses dados do IBGE e encontrei algumas informações bastante interessantes e úteis, mas surgiram novos problemas. Mesmo conectando os dados usando as chaves Município-Estado, as informações que consegui abrangiam apenas cerca de 4.300 municípios, enquanto a base do CNES possui pelo menos 5.600. A partir disso, busquei alternativas, e foi aí que encontrei uma solução muito interessante: a biblioteca chamada Geopy.

A biblioteca Geopy foi inicialmente desenvolvida por Kostya Esmukov em 2008. Ela é mantida como um projeto de código aberto e conta com a ajuda de colaboradores no GitHub. A Geopy é amplamente usada em projetos que necessitam de geocodificação e outras operações geográficas de maneira prática e eficiente, e vem sendo atualizada e aprimorada pela comunidade ao longo dos anos.

Aqui vão algumas curiosidades interessantes para quem quer saber mais:

1. Amplamente Compatível: O Geopy é compatível com diferentes provedores de serviços de geocodificação, como Google, OpenStreetMap, MapQuest, Bing, entre outros. Isso significa que você pode alternar entre eles ou utilizar múltiplos provedores para criar soluções de fallback caso um deles fique indisponível.

2. Controle de Taxa e Tolerância a Falhas: Muitas APIs de geocodificação, especialmente as gratuitas, impõem limites de requisições por segundo ou por dia. O Geopy oferece recursos como o RateLimiter e o tratamento de exceções específicas (como GeocoderUnavailable) para permitir a criação de aplicações resilientes e escaláveis. Isso é ideal para projetos que precisam fazer milhares de requisições em lote, mas devem respeitar os limites de taxa.

3. Geocodificação Direta e Reversa: Uma das funções mais populares do Geopy é a capacidade de fazer geocodificação direta (transformando endereços em coordenadas) e geocodificação reversa (transformando coordenadas em endereços). Isso é muito útil para projetos de análise de dados espaciais, permitindo criar mapas ou cruzar informações entre coordenadas e descrições de locais.

4. Integração Fácil com Pandas: Muitos usuários utilizam o Geopy junto com a biblioteca Pandas para processar dados geográficos em DataFrames. Ao integrar o Geopy e o RateLimiter, é possível geocodificar colunas inteiras de dados de endereço de forma eficiente.

5. Ferramenta de Desenvolvimento e Pesquisa: O Geopy é utilizado por muitos pesquisadores e estudantes, pois oferece uma maneira acessível de trabalhar com dados geográficos sem custos adicionais (quando usado com provedores gratuitos, como o OpenStreetMap).

Para quem deseja aprender mais ou contribuir para o projeto, o código-fonte do Geopy está disponível no GitHub, onde a comunidade pode sugerir melhorias, corrigir bugs e ajudar a expandir a biblioteca.

A biblioteca Geopy é uma ferramenta poderosa para engenheiros de dados e analistas que trabalham com dados geográficos e necessitam de coordenadas ou informações de localizações. Abaixo, um resumo das principais funções apresentadas:

Nominatim:

- Essa é uma classe de geocodificação da Geopy que permite traduzir endereços, cidades ou outras descrições geográficas em coordenadas de latitude e longitude. Ela usa o serviço OpenStreetMap, que é gratuito e oferece uma boa cobertura de dados.
Para usá-la, é preciso criar uma instância do Nominatim, fornecendo um parâmetro user_agent (um identificador único de cliente). Exemplo:

```py
geolocator = Nominatim(user_agent="myGeocoder")
location = geolocator.geocode("New York")
print(location.latitude, location.longitude)
```
- Com o Nominatim, podemos explorar diferentes formas de geocodificação, como transformar endereços em coordenadas (geocodificação direta) ou o inverso, convertendo coordenadas em endereços (geocodificação reversa).

GeocoderUnavailable:

- Esse é um tipo de exceção que a Geopy lança quando o serviço de geocodificação está indisponível (por exemplo, devido a um problema de rede ou limitação do serviço). Isso permite que lidemos melhor com falhas de conexão ou indisponibilidade temporária do serviço de geocodificação.

- Usar o GeocoderUnavailable é útil em aplicações que precisam ser resilientes a erros, permitindo a implementação de estratégias de reenvio ou execução de ações alternativas caso o geocodificador esteja indisponível.

RateLimiter:

O RateLimiter é uma ferramenta essencial para lidar com limitações de taxa ao usar o Nominatim (ou outro serviço de geocodificação), especialmente em aplicações que precisam fazer muitas requisições. Ele evita que você exceda o limite de taxa, pausando automaticamente as requisições conforme necessário.
Exemplo de uso com RateLimiter:
```py
from geopy.extra.rate_limiter import RateLimiter
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
location = geocode("New York")
```

Com o RateLimiter, é possível explorar processamento em batch de geocodificação, garantindo que as requisições respeitem os limites do serviço e evitando bloqueios.
Em resumo, a Geopy é excelente para enriquecer bases de dados com informações geográficas e para aplicações que precisam de dados de localização em grande escala com uma taxa de requisições controlada.

Para realizar esse enriquecimento, utilizei uma DAG específica em Python, pois, se usássemos PySpark, seria necessário aplicar UDFs, o que não seria vantajoso neste caso. O processo de enriquecimento basicamente usa o nome da cidade e o estado para obter as coordenadas do município. Como temos cerca de 5.600 cidades, esse processo foi um pouco demorado, levando cerca de 2 horas para obter todas as informações. No final, acredito que apenas algumas poucas cidades ficaram sem as coordenadas.

A função Python utilizada foi a seguinte:

```py
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
from requests.exceptions import ReadTimeout
from geopy.extra.rate_limiter import RateLimiter

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
```

Fluxo da Função
1. Configuração do Geocodificador:

- Inicializa o objeto Nominatim com o user_agent="municipios_ibge" e timeout=10 segundos para permitir respostas mais lentas.
- Utiliza o RateLimiter com min_delay_seconds=1.0 para garantir que as requisições sejam feitas de maneira controlada.

2. Consulta e Leitura dos Dados:

- Realiza uma consulta SQL para buscar dados de municípios na tabela CNES_tb_municipio da camada bronze.
Configura Municipio e Sigla, que são as colunas do DataFrame contendo o nome do município e a sigla do estado, respectivamente.
3. Processamento dos Dados:

- Cria listas para armazenar as latitudes e longitudes obtidas.

- Itera sobre as linhas do DataFrame e, para cada município, utiliza o geocode para obter as coordenadas baseadas no nome e estado do município.
- Em caso de sucesso, as coordenadas são adicionadas às listas latitudes e longitudes.
- Em caso de falha (exceções GeocoderUnavailable ou ReadTimeout), adiciona None às listas, indicando falha na obtenção das coordenadas.

4. Adição das Colunas no DataFrame:

- Adiciona as listas latitudes e longitudes como novas colunas Latitude e Longitude no DataFrame df.

5. Inserção no Banco de Dados:

- Insere o DataFrame atualizado na tabela CNES_tb_municipio da camada silver no banco de dados, utilizando df.to_sql.

Tratamento de Erros

- A função captura exceções GeocoderUnavailable e ReadTimeout, registrando o problema e continuando com o próximo item na lista.

Observações

- A função usa uma conexão previamente definida com o banco de dados e as funções auxiliares rodarQuery para executar a consulta SQL e inserirDadosDf para inserir o DataFrame no banco de dados.
- A escolha de RateLimiter evita o bloqueio por requisições excessivas, especialmente importante ao trabalhar com o Nominatim, que tem limites de taxa para requisições.

Dependências

Esta função depende das bibliotecas:

- Geopy para a geocodificação.
- SQLAlchemy ou outra biblioteca para a inserção do DataFrame no banco de dados com to_sql.
- Funções auxiliares rodarQuery e inserirDadosDf, que precisam estar definidas para a consulta e a inserção de dados.

## Tratamento e conversão geral de dados utilizando PySpark

Para as próximas tabelas que iremos utilizar nas análises, desenvolvi uma função genérica em PySpark para tratar e inserir esses dados na camada Silver. Nessa etapa, o PySpark avalia o tipo de cada coluna e realiza as conversões necessárias para a inserção correta no banco de dados. As tabelas tratadas aqui servirão como dimensões e filtros para nossas análises.


```py
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

```

A tabela que iremos trabalhar especificamente nesse caso é a `CNES_tb_estabelecimento` nela precisamos tratar o campo de longitude e latitude e outros campos caso seja necessário.

Para realizarmos os tratamentos desta tabela, temos campos de data que vêm em formatos diferentes. Nesses casos, utilizaremos o PySpark para realizar as conversões. Aproveitando, também faremos o join com a tabela de municípios e trataremos os estabelecimentos que não possuem coordenadas geográficas ou que possuem coordenadas inválidas.

Para a conversão de datas, utilizaremos as funções a seguir:

```py
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
```
- Função: convert_date_format

Essa função converte valores de uma coluna do tipo string para o formato de data padrão (yyyy-MM-dd) no PySpark. Ela verifica o formato atual da string para determinar como realizar a conversão. Caso o formato não seja reconhecido, o valor original da coluna é mantido.

Parâmetros:
- column (pyspark.sql.column.Column): Coluna cujos valores precisam ser convertidos.

Retorno:

- pyspark.sql.column.Column: Coluna com valores convertidos para o formato de data ou mantidos como estão, se o formato não for reconhecido.

Lógica:

1. Se o valor estiver no formato dd/MM/yyyy (contendo /), será convertido para yyyy-MM-dd usando a função to_date.
2. Se o valor estiver no formato dd-MMM-yyyy HH:mm:ss (contendo -), será convertido para yyyy-MM-dd usando a função to_date.
3. Caso o formato não corresponda a nenhum dos acima, o valor original será mantido.

- Função: try_cast_numeric

Essa função tenta converter os valores de uma coluna do tipo string para um valor numérico. Caso a conversão falhe, retorna None.

Parâmetros:

- column (str): Nome da coluna a ser convertida para numérico.

Retorno:

- pyspark.sql.column.Column: Coluna com valores convertidos para tipo numérico (float), ou None em caso de falha na conversão.

Lógica:

1. Verifica se o valor pode ser convertido para float utilizando cast("float").
2. Se a conversão for bem-sucedida (isNotNull), o valor original da coluna é mantido.
3. Caso contrário, retorna None.

A tabela final foram selecionadas as colunas que mais faziam sentido e realizei os tratamentos utilizando o seguinte codigo:

```py
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
```
# Função: `estabelecimentoEnriquecimento`

Essa função realiza o enriquecimento dos dados da tabela `CNES_tb_estabelecimento` utilizando informações da tabela `CNES_tb_municipio`. Após o processamento, os dados enriquecidos são armazenados no banco de dados PostgreSQL na camada `silver`.

## Etapas da Função:

1. **Configuração de Conexão com o Banco de Dados**:
   - Obtém o `jdbc_url` e as `jdbc_properties` por meio da função `connPsql`.

2. **Carregamento de Dados**:
   - Carrega as tabelas `bronze.CNES_tb_estabelecimento` e `silver.CNES_tb_municipio` do banco de dados PostgreSQL em DataFrames PySpark.

3. **Conversão de Datas**:
   - Converte as colunas de data `DT_EXPEDICAO` e `DT_ATUALIZACAO_ORIGEM` do DataFrame de estabelecimentos para o formato padrão `yyyy-MM-dd` usando a função `convert_date_format`.

4. **Join entre as Tabelas**:
   - Realiza um join à esquerda entre as tabelas `CNES_tb_estabelecimento` e `CNES_tb_municipio`, utilizando o código do município (`CO_MUNICIPIO_GESTOR` na tabela de estabelecimentos e `CO_MUNICIPIO` na tabela de municípios).

5. **Tratamento de Latitude e Longitude**:
   - Para as colunas `NU_LATITUDE` e `NU_LONGITUDE`:
     - Se o valor não for numérico, substitui pelas coordenadas correspondentes do município.
     - Caso contrário, mantém o valor original.

6. **Seleção de Colunas**:
   - Seleciona as colunas relevantes para o resultado, incluindo dados enriquecidos de latitude e longitude.

7. **Escrita no Banco de Dados**:
   - Salva os dados enriquecidos na camada `silver` do banco de dados PostgreSQL, sobrescrevendo a tabela correspondente.

8. **Finalização**:
   - Exibe o resultado do DataFrame processado e retorna uma mensagem indicando o sucesso da operação.

## Parâmetros
- **Nenhum**: A função não recebe parâmetros de entrada.

## Retorno
- **Nenhum**: A função não retorna valores explícitos, apenas exibe uma mensagem indicando o sucesso da operação.

---

## Dependências

- Funções necessárias:
  - `connPsql`: Configuração da conexão com o banco de dados.
  - `convert_date_format`: Conversão de strings para o formato de data.
  - `try_cast_numeric`: Validação de valores numéricos.

---

Com isso os dados de estabelecimento do CNES estão prontos para enviarmos para a camada gold e criarmos nossas análises

- [INSTALAÇÃO](https://github.com/Jezandre/eng_dados_cnes/blob/main/INSTALACAO.md)
- [DATA_LAKE](https://github.com/Jezandre/eng_dados_cnes/blob/main/CRIANDO_DATA_LAKE.md)
- [CAMADA_BRONZE](https://github.com/Jezandre/eng_dados_cnes/blob/main/CRIANDO_CAMADA_BRONZE.md)
- [ENTENDENDO_OS_DADOS](https://github.com/Jezandre/eng_dados_cnes/blob/main/ENTENDENDO_OS_DADOS.md)
- [PERGUNTAS_DE_NEGOCIO]()
- [CAMADA_SILVER]()
- [CAMADA_GOLD]()
- [DASHBOARD]()
