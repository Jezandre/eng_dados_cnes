# Criação do Data Lake de dados do CNES - Portal Data SUS

Há um certo tempo atrás eu já havia desenvolvido essa rotina para realizar o download dos dados. Então vou reaproveitar essa rotina para esse ponta pé inicial do projeto.

O CNES como já falei é um portal nacional para estabelecimentos de Saúde, e ele disponibiliza para quem quiser o download dos seus dados em um servidor ftp. Vocês podem visualizar isso em:

[Base de dados CNES](https://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp)

Para quem tem sempre a desculpa onde eu obtenho dados reais pra fazer meus projetos fica a dica. É possível criar diversas análises e montar alguns insights a partir deles, seja na questão mercadológica seja na questão de saúde pública.

Quando trabalhei com essas bases o desafio era automatizar, de maneira que os dados ficassem sempre atualizados e disponíveis sem a minha intervenção. O CNES disponibiliza os dados mensalmente na plataforma compactados em formato CSV. Se não me engano são mais de 50 ou 100 tabelas, então a princípio o desafio é pegar sempre a base mais atualizada.

Nesse primeiro passo vamos desenvolver uma rotina em python em conjunto com o airflow, uma maneira de sempre pegar o mais atual dos arquivos, decompactá-los e renomeá-los.

# Mãos a obra

As configurações já foram descritas no post anterior então vamos direto para a organização da estrutura que utilizaremos.

Como boa prática a meu ver é sempre bom utilizar arquivos diferentes para coisas diferentes e diretórios diferentes também.

Então teremos a seguinte estrutura de pasta:

- cnes_csv_files: Local onde vamos salvar os arquivos do CNES.

- dags: Local onde ficará os arquivos de dag

- dags/Python_Funcions: Local onde vamos colocar as funções que utilizaremos no Python Operator

- dags/Pyspark: Local onde iremos salvar os arquivos de execução do PySpark

Em resumo tudo que é função do Python ficará em Python Funcions e é a partir daí que iremos iniciar.

# DAGS

As dags tem o seguinte padrão de configuração:
```py
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
import os
import urllib.request
from datetime import datetime
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text 
import shutil
from urllib.error import URLError
import configparser
from Python_Functions.utils import *

default_args={
    'depends_on_past': False,
    # 'email': ['<<Coloque o email de notificação aqui>>'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 14),
    'catchup': False
}

dag = DAG(
    'cnes_files',
    description='Dados do dataSUS para download',
    schedule_interval=None,
    start_date=datetime(2023,3,5),
    catchup=False,
    default_args=default_args,
    default_view='graph',
    doc_md='DAG para registrar dados de unidades de saúde'
    )
```
## Pegar link para download

A primeira dag que iremos utilizar será responsável pela identificação do link de Download. O CNES utiliza um padrão da seguinte maneira:

```ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_<ANOMES>```

Então a lógica basicamente é identificar o mês e o ano mais recente em que os dados estão disponíveis. Como falei anteriormente os dados são disponibilizados mensalmente.

A primeira função que utilizaremos tem a funcionadlidade de identificar se a URL está disponível. Nela temos um time out, pois por mais que o python consiga identificar que aquele link existe, ele fica travado e quando ele fica travado é porque o link de fato existe:

```py
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
```
Parâmetros:

- url (string): O endereço URL a ser testado, como um FTP ou HTTP.
timeout (int, opcional): Tempo limite, em segundos, que a função espera para estabelecer uma conexão com a URL. O valor padrão é 10 segundos.

Retornos:

- True: Se a URL é acessível dentro do tempo especificado.
- False: Se a URL não pode ser acessada, seja por erro na conexão ou por exceder o tempo limite.

Tratamento de Exceções:

A função é projetada para lidar com dois tipos de exceções:

- urllib.error.URLError: Esta exceção ocorre se houver um erro de rede, como URL inválida ou problemas com a conexão. Nesse caso, o motivo da falha é capturado e impresso, e a função retorna False.

- Exception: Captura qualquer outro erro inesperado que possa ocorrer durante a tentativa de conexão, garantindo que a função sempre retorne False sem interromper o fluxo do programa.

A segunda função é a que retornará os valores de url, pasta de download e nome do local e armazenará esses nomes para o próximo passo que será a extração.

```py
def testarURL():
    data_atual = datetime.now()
    # Definir a pasta de destino
    pasta_destino = r'/path/para/arquivo/airflow/cnes_csv_files'

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
```
**Fluxo da Função:**

1. **Data Atual**:
   A função começa pegando a data atual com a biblioteca `datetime`. A cada iteração, a data é subtraída em um mês até que a URL válida seja encontrada ou o número máximo de tentativas seja alcançado (12 tentativas, retrocedendo até um ano).

2. **Pasta de Destino**:
   Define um caminho local onde o arquivo seria salvo se fosse baixado, que será retornado como parte da função.

3. **Loop com Limite de Tentativas**:
   O loop é limitado a 12 tentativas (equivalente a 12 meses anteriores). A cada iteração, a função gera uma nova URL baseada no ano e mês atual e tenta verificar se o arquivo existe usando a função `verifica_url_existe`.

4. **Verificação da URL**:
   A URL para o arquivo ZIP é gerada com base no ano e mês atual no formato:

A função `verifica_url_existe` é chamada para testar se essa URL está acessível.

5. **Retorno de Valores**:
- Se a URL for encontrada, a função retorna:
  - A **URL** acessível do arquivo.
  - O **caminho completo** onde o arquivo ZIP seria armazenado.
  - O **nome local do arquivo** para download.

- Se a URL não for encontrada, a data atual é subtraída em um mês, e o processo é repetido até atingir o número máximo de tentativas.

6. **Encerramento**:
- O loop para assim que a URL válida é encontrada e retorna os valores desejados.

**Parâmetros**:

- **Nenhum**: A função não recebe nenhum parâmetro, mas usa a data atual e um limite de tentativas (12 meses).

**Retornos:**

- **`url` (string)**: A URL do arquivo encontrado.
- **`caminho_zip` (string)**: O caminho onde o arquivo seria salvo localmente.
- **`local_filename` (string)**: O nome do arquivo ZIP a ser salvo.

A task para no arquivo dags ficará da seguinte maneira:

```py
pegar_url_task = PythonOperator(
    task_id = 'pegar_url',
    python_callable = testarURL,
    provide_context = True,
    dag=dag
)
```

- **task_id**: 
  - `pegar_url`: Identificador único da task dentro do DAG (Directed Acyclic Graph). Este ID é usado para referenciar a task em outras partes do DAG ou em logs.

- **python_callable**: 
  - `testarURL`: A função que será chamada quando a task for executada. Essa função tenta encontrar a URL de um arquivo ZIP correspondente à base de dados mais recente no servidor FTP.

- **provide_context**: 
  - `True`: Esta configuração permite que o contexto da execução (incluindo informações sobre a DAG, execução e outros parâmetros) seja passado para a função `testarURL`. Isso é útil para obter informações adicionais durante a execução, se necessário.

## Realizando o download e descompatando para o DataLake

Link definido, o próximo passo agora é realizar o download do arquivo. Em média o tamanho dos arquivos chegam a mais de 500. Então o doanload pode demorar um pouquinho.

O arquivo é baixado no formato ZIP. O Script que realiza isso é o seguinte:

```py
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
```

Fluxo de Execução:

1. **Contexto de Execução**:
   - A função recebe parâmetros usando `**kwargs`, que incluem informações de contexto de execução. O objeto `ti` (task instance) é extraído para permitir a comunicação entre tasks.

2. **Recuperação de Valores**:
   - A URL do arquivo, o caminho do ZIP e o nome do arquivo local são recuperados usando `ti.xcom_pull`, que busca dados de uma task anterior (neste caso, `pegar_url`).

3. **Definição da Pasta de Destino**:
   - O caminho da pasta onde os arquivos CSV extraídos serão armazenados é obtido a partir de uma variável de ambiente do Airflow (`path_file_cnes`).

4. **Verificação e Exclusão da Pasta de Destino**:
   - A função verifica se a pasta de destino já existe. Se existir, ela será excluída para garantir que os dados sejam atualizados. Isso é feito usando `shutil.rmtree`.

5. **Download do Arquivo**:
   - A função inicia o download do arquivo ZIP usando `urllib.request.urlopen`. O arquivo é salvo no caminho especificado por `local_filename`. 
   - O progresso do download é impresso no console.

6. **Criação da Pasta de Destino**:
   - Após o download, a função verifica se a pasta de destino existe. Se não existir, ela é criada com `os.makedirs`.

7. **Extração do Conteúdo do ZIP**:
   - O arquivo ZIP baixado é aberto e seu conteúdo é extraído para a pasta de destino usando `zipfile.ZipFile`.

8. **Informações Finais**:
   - O diretório atual após a extração é impresso no console, juntamente com uma mensagem indicando que os arquivos foram extraídos com sucesso.

A task que utilizaremos na DAG fica da seguinte forma:

```py
baixar_arquivos_task = PythonOperator(
    task_id='baixar_arquivos',
    python_callable=baixarArquivosCSV,    
    provide_context=True,
    dag=dag
)
```

## Renomeando os arquivos

Os arquivos quando descompactados eles vem com o seguinte padrão:

`<nomeTable><MesAno>.csv`

Nesse caso a gente não quer o mês ano, por isso iremos utilizar essa etapa. Talvez ela seja desnecessária, pois nas próxmias etapas poderíamos apenas ignorar os número, mas por via das dúvidas eu irei utilizar esse processo.

O código consiste nada mais nada menos que um for que rastreia todos os dados na pasta e renomeia os arquivos removendo o `AnoMês`. 

```py
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
```

**Fluxo de Execução:**

1. **Contexto de Execução**:
   - A função recebe parâmetros usando `**kwargs`, que podem incluir informações de contexto. Neste caso, ela utiliza uma variável de ambiente do Airflow para determinar a pasta que contém os arquivos a serem renomeados.

2. **Recuperação do Caminho da Pasta**:
   - O caminho da pasta onde os arquivos estão localizados é obtido usando `Variable.get("path_file_cnes")`.

3. **Início do Processo de Renomeação**:
   - A função imprime uma mensagem no console indicando o início do processo de renomeação, juntamente com a data e hora atuais.

4. **Definição do Padrão para Números**:
   - Um padrão regex é definido usando `re.compile(r'\d+$')`, que procura por dígitos (`\d`) que estejam localizados no final do nome do arquivo.

5. **Iteração Sobre os Arquivos**:
   - A função itera sobre todos os arquivos no diretório especificado usando `os.listdir(pasta)`.

6. **Verificação de Arquivos**:
   - Para cada item no diretório, a função verifica se é um arquivo regular usando `os.path.isfile(caminho_completo)`.

7. **Extração do Nome e Extensão do Arquivo**:
   - O nome do arquivo é dividido em nome (sem extensão) e extensão usando `os.path.splitext(nome_arquivo)`.

8. **Remoção de Números**:
   - O padrão de números é usado para remover quaisquer números do final do nome do arquivo com `re.sub(padrao_numeros, '', nome_sem_extensao)`.
   - Os espaços em branco extras são removidos do novo nome usando `novo_nome.strip()`.

9. **Construção do Novo Nome do Arquivo**:
   - O novo nome do arquivo é combinado com sua extensão original usando `f"{novo_nome}{extensao}"`.

10. **Renomeação do Arquivo**:
    - O arquivo é renomeado usando `os.rename(caminho_completo, novo_caminho)`.

11. **Finalização do Processo**:
    - Após renomear todos os arquivos, a função imprime uma mensagem no console indicando que o processo de renomeação foi finalizado, juntamente com a data e hora atuais.



```python
renomear_task = PythonOperator(
    task_id='renomear_arquivos',
    python_callable=renomearArquivos,
    provide_context=True,
    dag=dag
)
```

A partir daqui temos o nosso Data lake. O próximo passo é definir como inserir os dados dentro de um banco de dados, o objetivo é utilizar o DBT para este processo através do método COPY. Outras informações importantes é navegarmos dentro dos dados para entender a estrutura e os campos que eles nos disponibiliza.