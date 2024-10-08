# O que é o CNES e pra que ele serve

Para entendermos os dados com os quais iremos trabalhar, o primeiro e mais importante passo é entender o que é o CNES.

O CNES (Cadastro Nacional de Estabelecimentos de Saúde) é um sistema criado pelo Ministério da Saúde no Brasil para registrar todos os estabelecimentos de saúde, tanto públicos quanto privados, que prestam serviços à população. Esse cadastro existe para garantir uma organização e um controle eficiente do setor de saúde, oferecendo dados cruciais para o planejamento e a gestão dos serviços de saúde em nível municipal, estadual e federal.

## Por que o CNES existe?
O principal objetivo do CNES é centralizar informações detalhadas sobre todos os estabelecimentos de saúde, incluindo hospitais, clínicas, laboratórios, consultórios, entre outros. Com isso, é possível:

- Realizar um planejamento adequado dos recursos de saúde pública.
- Monitorar a capacidade instalada e os serviços prestados em diferentes regiões.
- Facilitar o acesso a dados que permitem a melhoria contínua dos serviços.
- Otimizar a alocação de recursos e o desenvolvimento de políticas públicas mais eficientes.

## O CNES é obrigatório?

Sim, o cadastro no CNES é obrigatório para todos os estabelecimentos de saúde no Brasil, sejam eles públicos, privados ou conveniados com o Sistema Único de Saúde (SUS). A falta de regularização pode acarretar em dificuldades para o estabelecimento atuar ou mesmo em penalidades, como a perda do direito de prestar serviços ao SUS e de receber repasses financeiros.

## Vantagens de possuir um número CNES

Legalização: A obtenção do CNES garante que o estabelecimento está operando dentro das normas vigentes, o que é essencial para prestar serviços de saúde de forma regular.
Acesso a convênios: O cadastro é um requisito para o estabelecimento firmar convênios com o SUS, o que pode aumentar o fluxo de pacientes e garantir repasses financeiros.

Transparência e visibilidade: Estabelecimentos cadastrados no CNES têm seus dados acessíveis ao público, o que pode conferir maior visibilidade e confiança por parte dos cidadãos e outros órgãos reguladores.

Como o cidadão pode utilizar essas informações?
Como cidadão, você pode utilizar o CNES para acessar informações detalhadas sobre os serviços de saúde disponíveis na sua região. Pelo portal do CNES, é possível:

## Consultar quais estabelecimentos estão registrados em sua cidade ou estado.

Verificar quais serviços são prestados por cada estabelecimento (atendimentos, especialidades, etc.).

- Conhecer a capacidade instalada de hospitais e clínicas, como quantidade de leitos, equipamentos e profissionais disponíveis.
- Avaliar se o estabelecimento que você frequenta está devidamente regularizado e atende aos requisitos exigidos pelo Ministério da Saúde.

Esse tipo de consulta é importante para que o cidadão faça escolhas informadas sobre onde buscar atendimento e tenha uma noção da infraestrutura disponível para a população em geral.


Aposto que você não conhecia essa fonte de informação, mas o CNES é uma ferramenta poderosa para quem busca entender melhor o sistema de saúde no Brasil. Além de ser um cadastro obrigatório para todos os estabelecimentos de saúde, ele proporciona uma visão detalhada sobre a estrutura e os serviços de cada unidade. Como cidadão, você pode usar esses dados para tomar decisões mais informadas sobre onde buscar atendimento e garantir que está sendo atendido em um estabelecimento devidamente regularizado. O CNES fortalece a transparência e melhora o planejamento da saúde no país.

# Entendendo as tabelas disponíveis

O CNES disponibiliza em seu portal toda a documentação e o dicionário de dados, o que facilitará significativamente o nosso processo de modelagem. Com essas informações, poderemos compreender melhor e estruturar as análises que iremos realizar.

O site é o seguinte:

[Documentação portal CNES](https://cnes.datasus.gov.br/pages/downloads/documentacao.jsp)

![image](https://github.com/user-attachments/assets/b0054b4c-4658-44a8-b990-b3eb54f93eaf)


A primeira tabela que será importante para o nosso trabalho se chama tabela domínio. Nela, temos as informações essenciais para iniciarmos as atividades. Através dela, é possível identificar e localizar o "de/para" necessário para as tabelas em um único arquivo.

![image](https://github.com/user-attachments/assets/add2d79c-5030-4c7b-b2c0-9d1be9ae0c79)


A tabela Domínio do CNES serve para padronizar e categorizar informações específicas dentro do sistema. Ela contém códigos e descrições padronizados para diferentes classificações, como tipos de serviços, categorias de estabelecimentos, e outras variáveis que precisam ser uniformemente referenciadas nas demais tabelas do sistema CNES.

Essencialmente, as tabelas de domínio são utilizadas para garantir consistência nas informações e facilitar a consulta e o cruzamento de dados dentro do sistema. Elas servem como uma espécie de dicionário ou tabela de referência, associando códigos a descrições mais detalhadas. Por exemplo, ao invés de armazenar o nome de cada tipo de serviço de saúde diretamente nas tabelas, o CNES armazena um código de referência que corresponde a uma entrada na tabela Domínio.

Funções principais da tabela Domínio:
- **Padronização**: Garante que os dados sejam registrados de forma consistente em todo o sistema.
- **Referência**: Fornece uma lista de opções válidas para diferentes campos do sistema, como tipos de serviços ou categorias de profissionais.
- **Facilita a consulta**: Simplifica a estrutura dos dados, tornando mais fácil realizar consultas e relatórios padronizados.

Com essa estrutura, analistas e gestores de saúde podem utilizar os códigos de domínio para fazer análises e relatórios, garantindo que as classificações sejam consistentes em diferentes regiões e contextos.

## Dicionário de dados 

O dicionário de dados apresenta todas as informações sobre os campos e tabelas disponíveis para iniciarmos a modelagem dos dados. Através dele, podemos nos orientar pelos tipos de dados e pelas informações disponíveis.

No CNES, as tabelas são distribuídas em dois grupos principais: TB e RL. Esses prefixos indicam o tipo de dado ou relacionamento que cada tabela armazena.

**TB (Tabela Básica)**: Essas tabelas contêm informações principais, mais estáticas ou "básicas" dos estabelecimentos de saúde. Elas armazenam dados descritivos, como o nome do estabelecimento, endereço, tipo de atendimento, entre outros. São as tabelas que centralizam as características mais fundamentais de cada entidade no sistema CNES.

**Exemplo**:

TB_ESTABELECIMENTO: Contém os dados básicos de cada estabelecimento de saúde.

**RL (Relacionamento)**: As tabelas RL são utilizadas para representar relacionamentos entre as entidades armazenadas nas tabelas TB. Elas servem para mapear interações ou conexões entre diferentes tabelas, como, por exemplo, o vínculo de profissionais com os estabelecimentos de saúde, ou a relação de equipamentos com um determinado hospital.

**Exemplo**:

RL_ESTAB_PROFISSIONAL: Representa o relacionamento entre um estabelecimento e seus profissionais de saúde.

**Resumindo**:

TB: Tabelas que contêm os dados principais, geralmente descritivos.

RL: Tabelas que registram os relacionamentos entre diferentes entidades, mostrando como elas se conectam no sistema.

Em um modelo dimensional, as nossas tabelas TB correspondem às tabelas dimensão, enquanto as tabelas RL são as tabelas fato.

Com o conhecimento dos dados que temos, agora podemos trabalhar para criar nossos modelos de dados, baseando-nos nas perguntas de negócio que queremos responder a partir dessas informações.

Continue seguindo, pois ainda há bastante conteúdo interessante a ser explorado.

- [INSTALAÇÃO](https://github.com/Jezandre/eng_dados_cnes/blob/main/INSTALACAO.md)
- [DATA_LAKE](https://github.com/Jezandre/eng_dados_cnes/blob/main/CRIANDO_DATA_LAKE.md)
- [CAMADA_BRONZE](https://github.com/Jezandre/eng_dados_cnes/blob/main/CRIANDO_CAMADA_BRONZE.md)
- [ENTENDENDO_OS_DADOS](https://github.com/Jezandre/eng_dados_cnes/blob/main/ENTENDENDO_OS_DADOS.md)
- [PERGUNTAS_DE_NEGOCIO]()
- [CAMADA_SILVER]()
- [CAMADA_GOLD]()
- [DASHBOARD]()
