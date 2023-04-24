# read_S3_alteryx
Projeto de Ingestão de dados do AWS S3 para o AWS Redshift.

Projeto desenvolvido com o objetivo de transportar dados que estão armazenados dentro de um Bucket no AWS S3 (Data Lake), para um AWS Redshift (Data Warehouse).

Tipos de arquivos .csv separados por vírgula situados em pastas distintas no bucket. Processo de consolidação de todos os arquivos mediante o caminho, através do nome de cada arquivo.

# Arquitetura de dados.
![image](https://user-images.githubusercontent.com/63620777/234071926-d3281215-c406-4a6f-89df-f55539c2dbcc.png)

O processo é interligado com o Alteryx Analytics, onde é responsável por fazer a transformação e a carga dos dados no banco de dados Redshift.

![image](https://user-images.githubusercontent.com/63620777/234072932-6d67cbbc-9ecb-44a8-94b1-43a3ea500bea.png)

Ou temos a opção de fazer o download de todos os arquivos de forma consolidada em .csv ou em qualquer arquivo desejado.

![image](https://user-images.githubusercontent.com/63620777/234075532-f27113e3-d6f2-4076-8e55-3bad32e6e3b7.png)

Para este caso fica aberto a opção da utilização da ferramenta de Analytics para carregamento no banco de dados Redshift. Logo que os arquivos estarão disponíveis no diretório local de sua máquina.
