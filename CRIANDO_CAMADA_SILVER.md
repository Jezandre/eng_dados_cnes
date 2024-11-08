# Enriquecendo a camada Silver com dados de coordenadas geográficas

A camada Silver é a etapa em que precisamos filtrar e limpar os dados para garantir uma análise precisa e confiável. Nessa fase, trataremos dados duplicados, inválidos, nulos, entre outros possíveis problemas. Neste caso, optei por utilizar o PySpark para essa tarefa, com o objetivo principal de demonstrar como essa ferramenta funciona em um contexto prático. Sei que os mais experientes podem questionar essa escolha, pois não estamos lidando com milhões de registros, mas acredito que é uma abordagem interessante para explorar o potencial do PySpark em uma base significativa de dados.

Portanto, para o tratamento das tabelas, utilizaremos o PySpark.

Nos dados do CNES, uma das análises que pretendo explorar está relacionada à localização dos estabelecimentos de saúde. Com esses dados, podemos obter insights valiosos sobre a distribuição geográfica desses estabelecimentos. Para isso, precisaremos analisar cuidadosamente as colunas e tabelas que trazem essas informações de localização e ver como estão estruturadas.

# Explorando os dados

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

Para realizar esse enriquecimento utilizei uma dag específica para isso utilizando Python visto que se fossemos utilizar PySpark aqui teriamos que utilizar UDF e para esse caso não seria necessário. O enriquecimento basicamente pega as informações do nome da cidade e do estado e retorna as coordenadas do municipio. Então como temos 5600 cidades nesse caso o processo foi um pouco oneroso, cerca de 2 horas para obter todos os dados. No fim acredito que apenas algumas poucas cidades eu não consegui encontrar as localizações.

A função em python utilizada foi a seguinte:

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