import boto3
import os
import sys
from config.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def upload_to_s3(local_file, bucket_name=AWS_BUCKET, s3_file=None):
    s3_file = s3_file or os.path.basename(local_file)
    try:
        print(f"Upload: {local_file} -> S3://{bucket_name}/{s3_file}")
        sys.stdout.flush()
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"Le fichier {local_file} n'existe pas")
        s3_client.upload_file(local_file, bucket_name, s3_file)
        print(f"Upload réussi: {s3_file}")
        sys.stdout.flush()
    except Exception as e:
        print(f"Erreur lors de l'upload: {e}")
        sys.stdout.flush()
        raise

def verify_s3_upload(bucket_name=AWS_BUCKET, s3_file=None):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_file)
        print(f"Vérification réussie: {s3_file} existe sur S3")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"Erreur lors de la vérification: {e}")
        sys.stdout.flush()
        raise

def download_from_s3(bucket_name=AWS_BUCKET, s3_file=None, local_file=None):
    try:
        s3_client.download_file(bucket_name, s3_file, local_file)
        print(f"Download réussi: S3://{bucket_name}/{s3_file} -> {local_file}")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"Aucune base existante trouvée sur S3 pour {s3_file}: {e}")
        sys.stdout.flush()
        return False