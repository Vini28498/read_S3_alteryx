# read_S3_alteryx
Projeto de Ingestão de dados do AWS S3 para o AWS Redshift.

Projeto desenvolvido com o objetivo de transportar dados que estão armazenados dentro de um Bucket no AWS S3 (Data Lake), para um AWS Redshift (Data Warehouse).

Tipos de arquivos .csv separados por vírgula situados em pastas distintas no bucket. Processo de consolidação de todos os arquivos mediante o caminho, através do nome de cada arquivo.

# Arquitetura de dados.
![image](https://user-images.githubusercontent.com/63620777/234071926-d3281215-c406-4a6f-89df-f55539c2dbcc.png)
