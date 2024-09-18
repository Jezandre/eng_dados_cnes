# Introdução

Esse projeto consistirá em uma construção de pipeline end to end da extração do dado até a injestão do mesmo nas camadas bronze, Silver e Gold.

Os dados a serem trabalhados virão DataSUS, são uma coleção de dados do Cadastro Nacional de Estbelecimento de Saúde. É uma releitura de um projeto que fiz há um tempo no primeiro local onde trabalhei com analista de dados.

Esses dados trazem informações importantes para empresas que trabalham com venda de produtos voltados para a saúde. Esse conjunto de dados ficam disponíveis em CSV dentro de um servido ftp, o que me aguçou a curiosidade na época, pois hoje em dia navegadores que utilizam ftp são raros. Além disso esses dados são atualizados mensamente, então uma automação para a coleta destes dados é muito interessante.

# Instalações

Mas chega de papo, esse primeiro artigo o objetivo é estabelecer a configuração das ferramentas de trabalho. O intuito aqui é aplicar conhecimento que obtive de Airflow, PySpark, Python, SQL, Snowflake e DBT.

- Airflow: Orquestrador de workflows que automatizará e agendará os pipelines de dados, gerenciando dependências entre tarefas.

- PySpark: Interface Python para o Apache Spark, usada para processamento de grandes volumes de dados de forma distribuída.

- Python: Linguagem de programação versátil usada para escrever scripts de ETL, automação e integração de ferramentas.

- SQL: Linguagem usada para consulta e manipulação de dados em bancos de dados relacionais.

- Snowflake: Plataforma de armazenamento de dados na nuvem que oferece escalabilidade, processamento de dados e integração com várias ferramentas.

- DBT (Data Build Tool): Ferramenta de transformação de dados que facilita a criação de modelos SQL versionados e automatizados em data warehouses.

As versões que utilizarei serão as seguintes

- python 3.11.10
- python venv python3.11
- Airflow 2.9.3
- DBT 1.0.0.38.13
- Pyspark 3.5.2

Outras ferramentas:

- Ubuntu 20.04.6 LTS

- Power BI: Ferramenta de visualização de dados.

- Git: para controle de versionamento e documentação

- (Talvez) Docker: Para dockerizar o projeto, digo talvez pq tive alguns probemas tentando utilizar o airflow por ai mas acho que como ultima etapa seria interessante

# Configurar o Ubuntu 20.04.6 LTS

- Instalação

A primera etapa da configuração é utilizar o `Linux Ubuntu 20.04.6 LTS`. Você pode utilizar de várias maneiras, criando uma instancia EC2 na Amazon ou qualquer outra nuvem que deseja utilizar. Pode instalar em uma máquina virtual em uma virtual Box. Acredito que esses são os meios mais eficazes de se utilizar a configuração.

No meu caso eu preferi utilizar o subsistema linux do windows ao qual chamamos de WSL. Isso pq na instalação do docker ele só funcionou na minha máquina utilizando o WSL. Achei uma solução interessante e que funcinou para o meu intuito.

```bash
wsl --install
```
Após a instalação no power shell você pode instalar o Ubunto da seguinte maneira:

```bash
wsl --install -d Ubuntu-20.04
```

Esse comando baixará e instalará o Linux na sua máquina.

Depois você precisará garantir que o sistema esteja atualizado e para isso utilize o seguinte comando no power shell.

```bash
sudo apt update && sudo apt upgrade -y
```
- Configurar usuário

Adicionar um novo usuário
```bash
sudo adduser nome_do_usuario
```
Acessar o usuário
```bash
su - nome_do_usuario
```
Sair do usuário

```bash
exit
```

- Acessar o Linux no VSCode

Utilizar o `VSCode` para realizar o acesso remoto será uma mão na roda. Para fazer isso basta instalar a extensão chamada `Remote Explorer`.

Acesse a extensão na lateral esquerda, e na parte superior esquerda selecione `WSL Targets` em `Remote Explorer` Clicar no `+` e selecionar o Linux que instalado no WSL. Em explorerer você visualizará os arquivos.

Linux instalado agora é hora de instalar as ferramentas que utilizaremos. Vamos começar pelo Python.

# Instalar o Python 3.11.10

Para instalar o `Python 3.11.10` basta utilizar o seguinte comando na comand line do Linux:

```bash
sudo apt install python3.11
```
Após a instalação sempre bom verificar se o python foi realmente instalada utilizando o  seguinte comando:

```bash
python3.11 --version
```

# Instalar dependencias e blibliotecas do python

Para esse projeto utilizamremos um ambiente virtual então é importante que `venv` seja instalado assim como as bibliotecas e o drive odbc.

O venv instalamos utilizando o seguinte comando:

```bash
sudo apt install python3.11-venv
```

O drive odbc você utiliza o seguinte comando para instalar dependencias:

```bash
sudo apt install unixodbc-dev
```
Em seguida os drivers do PostgreSQL e Snowflake
```bash
sudo apt install odbc-postgresql
```

```bash
sudo dpkg -i snowflake-odbc-<version>.deb
```

Depois dessas instalações precisamos criar e ativar o ambiente virual para instalarmos as bibliotecas e aplicações que iremos utilizar.

Para isso utilizaremos o seguinte comando, eu opter por chamar de airflow.

Comando para criar 

```bash
airflow_env -m venv myenv
```

Ativar

```bash
source airflow_env/bin/activate
```
Quando o ambiente está ativado você conseguirá ver o cursor da seguinte maneira:

```shell
(airflow_env) usuario@usuario:~/airflow$ 
```

Agora vamos instalar as bibliotecas principais que iremos utilizar:

Driver ODBC

```bash
pip install pyodbc
```
Pandas

```bash
pip install pandas
```
Conector Snowflake

```bash
pip install snowflake-connector-python
```
# Instalar postgres

Primeiramente atualizar o instalador

```bash
sudo apt update
sudo apt install -y wget ca-certificates
```

Baixar o repositório para instalarmos a versão quer queremos
```bash
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
```

Adicione o repositório de PostgreSQL ao seu sistema:
```bash
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs) pgdg" > /etc/apt/sources.list.d/pgdg.list'
```
Instale o PostgreSQL 12.9:
```bash
sudo apt install -y postgresql-12
```
Criar um novo usuário, acesse o usuário padrão do postgres

```bash
sudo -i -u postgres
```

Acesse o postgres utilizando o comando:

```bash
psql
```
Com seguintes comandos SQL crie seu usuário

```sql
CREATE USER airflow WITH PASSWORD 'airflow';
```

Crie a base de dados
```sql
CREATE DATABASE airflow_db;
```

Dê todas as permissões necessárias para o seu usuário
```sql
GRANT ALL PRIVILEGES ON DATABASE nome_do_banco TO nome_do_usuario;
```
Para sair do Postgres utilize
```sql
\q
```
E para sair do usuário o seguinte comando
```bash
exit
```
# Instalar Airflow

- Atualizar o flask:


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