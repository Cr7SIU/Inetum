import boto3
import os


def save_dataframe_to_s3(file_path, bucket_name: str, s3_path: str, aws_access_key: str, aws_secret_key: str) -> None:

    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )

    s3_client = session.client('s3')

    try:
        s3_client.upload_file(file_path, bucket_name, s3_path)
        print(f"Archivo guardado exitosamente en S3: s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            
    return  f"s3a://{bucket_name}/{s3_path}/{file_path}"