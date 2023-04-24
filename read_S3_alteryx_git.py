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
mapa_sua-tabela = {'sua-tabela.csv': 'sua-tabela_consolidado.csv'}

# Processo de leitura e consolidação do bucket
listas_arquivos_sua-tabela = {}
for nome_arquivo, nome_arquivo_consolidado in mapa_sua-tabela.items():
    # buscar todos os arquivos com o nome correspondente na pasta e subpastas
    listas_arquivos_sua-tabela = []
    for obj in acess['Contents']:
        key = obj['Key']
        if key.endswith(nome_arquivo) and key.split("/")[-1] == nome_arquivo:
            listas_arquivos_sua-tabela.append(key)
    df_sua-tabela_consolidado = pd.concat([pd.read_csv(io.StringIO(s3.get_object(Bucket=bucket_name, Key=arquivo)['Body'].read().decode('utf-8')), sep=';', encoding='latin-1', low_memory=False) for arquivo in listas_arquivos_sua-tabela])
    print(df_sua-tabela_consolidado)

# Gerar um dataframe na saída de conexão do Alteryx para seguir o fluxo de dados.
Alteryx.write(df_sua-tabela_consolidado,1)
