from minio import Minio
import os

minio_client = Minio(
    'AQUI VAI O IP DO MINIO:9000',
    access_key='USUARIO DO MINIO',
    secret_key='SENHA DO USUARIO DO MINIO',
    secure=False
)

bucket_name = 'mentalheath' # Nome do bucket no MinIO

# Criar o bucket se ele não existir
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Diretório onde os arquivos CSV foram salvos
diretorio_csv = '.'

# Fazer o upload dos arquivos CSV para o MinIO
for csv_file in os.listdir(diretorio_csv):
    if csv_file.endswith('.csv'):
        file_path = os.path.join(diretorio_csv, csv_file)
        object_name = f'dados/{csv_file}'
        minio_client.fput_object(bucket_name, object_name, file_path)
        print(f"Arquivo {csv_file} carregado com sucesso para o MinIO no bucket {bucket_name} com o nome {object_name}.")
        