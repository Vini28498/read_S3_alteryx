# Importando a biblioteca do Alteryx.
from ayx import Alteryx

# Importando as bibliotecas necessárias para o processo.
import boto3 
import io
import os
import pandas as pd

# Acessando o AWS S3.
s3 = boto3.client(
   's3',
   aws_access_key_id='sua-chave-id',
   aws_secret_access_key='sua-chave-de-acesso'
)

# Acessando Data Lake - Bucket.
bucket_name = 'seu-bucket-data-raw'
filtro_caminho = 'se-necessario'

# Denotando o diretório local.
diretorio_local = 'seu-diretorio-local'

# Listando os objetos dentro do S3 através de um caminho especificado.
acess = s3.list_objects(Bucket=bucket_name, Prefix=filtro_caminho)

# Visualizando todos os objetos que existam, filtrando todos aqueles que são arquivos .csv
if 'Contents' in acess:
   for obj in acess['Contents']:
      key = obj['Key']
      if key.endswith('.csv'):
         print(key)
else:
   print('Não foi possível acessar o bucket.')

# Mapeamento das tabelas necessárias.
mapa_nomes = {'sua-tabela1.csv': 'sua-tabela1_consolidado.csv',
              'sua-tabela2.csv': 'sua-tabela2_consolidado.csv',
              'sua-tabela3.csv': 'sua-tabela3_consolidado.csv',
              'sua-tabela4.csv': 'sua-tabela4_consolidado.csv',
              'sua-tabela5.csv': 'sua-tabela5_consolidado.csv',
              'sua-tabela6.csv': 'sua-tabela6_consolidado.csv',
              'sua-tabela7.csv': 'sua-tabela7_consolidado.csv'}

# Processo de leitura, consolidação e download dos arquivos do bucket
listas_arquivos = {}
for nome_arquivo, nome_arquivo_consolidado in mapa_nomes.items():
    # buscar todos os arquivos com o nome correspondente na pasta e subpastas
    lista_arquivos = []
    for obj in acess['Contents']:
        key = obj['Key']
        if key.endswith(nome_arquivo) and key.split("/")[-1] == nome_arquivo:
            lista_arquivos.append(key)
    listas_arquivos[nome_arquivo] = lista_arquivos
    df_consolidado = pd.concat([pd.read_csv(io.StringIO(s3.get_object(Bucket=bucket_name, Key=arquivo)['Body'].read().decode('utf-8')), sep=';', encoding='latin-1') for arquivo in lista_arquivos])
    print(df_consolidado)
    with open(f"{diretorio_local}/{nome_arquivo_consolidado}", 'w') as f:
        df_consolidado.to_csv(f, sep=';', index=False)