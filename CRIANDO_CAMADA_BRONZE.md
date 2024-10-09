# Criando a camada Bronze

Realizando o curso do Welikiandre, deparei-me com um conceito novo para mim que já faz parte da minha rotina de trabalho: as camadas bronze, silver e gold. Cada camada é responsável por uma etapa do processamento de dados.

A camada bronze é responsável pela coleta mais bruta possível dos dados e das informações. Podemos recorrer a ela quando precisarmos de informações e entender o que foi feito com os dados nas outras camadas.

Na camada silver, temos os primeiros tratamentos, e a camada gold é onde realizamos os agrupamentos, já pensando em tomada de decisão.

Nesse caso, partiremos para a inserção e escolha daqueles arquivos que fizemos o download nas etapas anteriores. A princípio, aqui vou mostrar apenas como é realizado o processo de inserção. Assim que definirmos as regras de negócio, partiremos para a exploração dos dados e a definição de quais tabelas iremos utilizar.
# Criando a rotina de inserção

Como expliquei na parte de criação do Data Lake, o CNES disponibiliza uma grande quantidade de tabelas e dados para que possamos utilizar em nossas análises. Nesse momento, nosso foco são duas coisas: criar a rotina que vai pegar uma dessas tabelas e, em seguida, inseri-la em um banco de dados. No primeiro momento, vou utilizar o PostgreSQL.

Para este processo, utilizaremos três tasks, as quais chamei de selecionar_arquivos_task, criar_tabelas_task e inserir_dados_task.


## selecionar_arquivos_task

Esta tarefa será responsável pela definição dos arquivos que iremos utilizar. No caso, ela funciona como um dicionário, onde trago o nome e o caminho do arquivo que irei usar e o nome da tabela na qual vamos inserir os dados. Para isso, ela utiliza um Python Operator, que utiliza a seguinte função.

```py
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
        'tb_servico_especializado': str(pasta_destino) + '/tbServicoEspecializado.csv'
    }

    return csv_files
```
Voce define os parâmetros e tabelas que irá utilizar e a função retorna os aquivos e dados.

A Task fica da seguinte maneira:

```py
selecionar_arquivos_task = PythonOperator(
    task_id='obter_arquivos_csv',
    python_callable=selecionarArquivosCSVutilizados,
    provide_context=True,
    dag=dag,
)
```

## criar_tabelas_task

Esta tarefa será responsável por visualizar o arquivo e criar a tabela no banco de dados com base nas informações observadas, como o nome da coluna e o nome da tabela. A princípio, todos os dados inseridos serão do tipo varchar. Farei isso porque, como a primeira etapa da camada bronze é ainda apenas uma camada de inserção, a identificação desses dados para inserir nesta etapa poderia gerar erros na inserção dos dados.

A função é a seguinte: ela pega as informações de caminho e nome da tabela a partir da task anterior e utiliza como referência para criar as tabelas de dados no PostgreSQL.

```py
def criarTabela(**kwargs):
    csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv')
    for table_name, file_path in csv_files.items():
        with open(file_path, 'r', encoding='latin-1') as file:
            columns = file.readline().strip().split(';')
            columns = [col.replace('"', '').strip() for col in columns]    
            columns_str = ', '.join([f'"{col}" VARCHAR' for col in columns])
            create_table_sql = f'CREATE TABLE IF NOT EXISTS "bronze"."CNES_{table_name}" ({columns_str});'
            create_table_task = PostgresOperator(
                task_id=f'create_table_{table_name}',
                postgres_conn_id='airflow',
                sql=create_table_sql,
            )
        create_table_task.execute(kwargs)
```
- Fluxo da task 

1. Recuperação dos arquivos CSV: A função utiliza xcom_pull para recuperar o dicionário csv_files, que contém os nomes das tabelas e os caminhos dos arquivos CSV, a partir de uma tarefa anterior (obter_arquivos_csv).

2. Leitura e processamento dos arquivos CSV: Para cada par table_name (nome da tabela) e file_path (caminho do arquivo CSV):

3. O arquivo CSV é aberto em modo de leitura, com codificação 'latin-1'.
A primeira linha do arquivo, que contém os nomes das colunas, é lida e separada usando o delimitador ;.
As aspas (") são removidas e os espaços extras são eliminados de cada nome de coluna.
Criação da query SQL:

4. Uma string com a definição das colunas é gerada, onde cada coluna é definida como VARCHAR.

5. A consulta SQL para criar a tabela no esquema bronze é montada no formato:
```sql
CREATE TABLE IF NOT EXISTS "bronze"."CNES_<table_name>" (<columns_str>);
```
6. Um objeto PostgresOperator é criado para executar a consulta SQL no banco de dados PostgreSQL.

7. A tarefa de criação da tabela é então executada através do método execute().

A task é definida desta forma:

```py
criar_tabelas_task = PythonOperator(
    task_id='criar_tabelas_from_csv',
    python_callable=criarTabela,
    dag=dag
)
```

## inserir_dados_task

A tarefa inserir_dados fará a leitura dos arquivos, transformará os dados em DataFrames e os inserirá no banco de dados utilizando Python. Os dados são inseridos em chunks de 1.000 para evitar que o computador sofra muito. Armazenar um DataFrame muito grande em memória traria sérios problemas, e como temos tabelas bem grandes, utilizar a inserção por pedaços vai evitar grandes erros.

No primeiro momento, eu iria fazer dessa maneira, mas fiz alguns testes e percebi que não é a melhor abordagem para a inserção de dados, sabendo que temos alguns recursos adicionais utilizando o PostgreSQL ou o Snowflake. Esses dois bancos têm uma estrutura chamada COPY, que funciona para arquivos. Essa função é uma mão na roda, pois, utilizando o COPY, o processo que estava levando mais de uma hora para inserir 500 mil linhas levou menos de um minuto.

O processo é o seguinte: primeiro, você precisa garantir que o usuário que acessa o banco tenha acesso a essa funcionalidade. Então, entre como administrador no psql e execute o seguinte comando:

```sql
GRANT pg_read_server_files TO airflow;
```

Ou então se você estiver em um banco que utiliza para estudos utilize o seguinte:

```sql
GRANT ALL PRIVILEGES ON DATABASE nome_do_banco TO airflow;
```

Feito isso vamos para os códigos a função ficará bem simples basta utilizar da seguinte maneira:

```py
def inserirDados(**kwargs):
    
    csv_files = kwargs['ti'].xcom_pull(task_ids='obter_arquivos_csv')
    hook = PostgresHook(postgres_conn_id='airflow')
    # Limpar dados anteriores
    for table_name, file_path in csv_files.items():
        hook.run(f'TRUNCATE TABLE "bronze"."CNES_{table_name}";')  
        hook.run(f"""COPY 
                    "bronze"."CNES_{table_name}" 
                    FROM \'{str(file_path)}\' 
                    DELIMITER \';\' 
                    CSV HEADER 
                    ENCODING \'LATIN1\'""")
```

O código sempre fará um refresh completo na tabela, isso porque a quantidade de atualizações dessa base não é significativa a ponto de onerar muito o sistema quando isso acontece. Em casos em que o sistema pode sofrer, é interessante pensar em atualizações incrementais.

Feito isso bora pra Task:
```py
inserir_dados_task = PythonOperator(
    task_id='inserir_dados',
    python_callable=inserirDados,
    provide_context=True,
    dag=dag
)

```

Tasks prontas, temos a nossa estrutura de inserção de dados na camada bronze pronta para as próximas etapas de tratamento. Próximo passo será entender um pouco melhor a estrutura dos dados fornecidos pelo CNES para que possamos criar perguntas de negócio e analisar os dados e tabelas que poderemos utilizar.

- [INSTALAÇÃO](https://github.com/Jezandre/eng_dados_cnes/blob/main/INSTALACAO.md)
- [DATA_LAKE](https://github.com/Jezandre/eng_dados_cnes/blob/main/CRIANDO_DATA_LAKE.md)
- [CAMADA_BRONZE](https://github.com/Jezandre/eng_dados_cnes/blob/main/CRIANDO_CAMADA_BRONZE.md)
- [ENTENDENDO_OS_DADOS](https://github.com/Jezandre/eng_dados_cnes/blob/main/ENTENDENDO_OS_DADOS.md)
- [PERGUNTAS_DE_NEGOCIO]()
- [CAMADA_SILVER]()
- [CAMADA_GOLD]()
- [DASHBOARD]()
