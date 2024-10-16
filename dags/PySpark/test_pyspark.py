import pandas as pd
from dbfread import DBF
import os
import requests
def dadosMunicipiosIBGE():
    # URL do arquivo a ser baixado
    url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/localidades/cadastro_de_localidades_selecionadas_2010/Shapefile_SHP/BR_Localidades_2010_v1.dbf"

    # Caminho completo onde o arquivo será salvo
    save_path = 'ibge_files/BR_Localidades_2010_v1.dbf'

    # Criar o diretório se não existir
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    try:
        # Fazer o download do arquivo
        response = requests.get(url)
        response.raise_for_status()  # Levanta um erro se a requisição falhar

        # Salvar o arquivo
        with open(save_path, 'wb') as file:
            file.write(response.content)
        
        print(f"Arquivo '{save_path}' baixado com sucesso.")

    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro ao baixar o arquivo: {e}")

    # Lê o arquivo DBF
    table = DBF(save_path, load=True, encoding = 'latin-1')

    # Converte para um DataFrame do pandas
    df = pd.DataFrame(iter(table))

    # Exibe o DataFrame





