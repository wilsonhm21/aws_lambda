import boto3
import csv
import json
import pymysql
import os

s3 = boto3.client('s3')

# Configuración para RDS
rds_host = os.environ['RDS_HOST']
rds_user = os.environ['RDS_USER']
rds_pass = os.environ['RDS_PASS']
rds_db   = os.environ['RDS_DB']

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']
    
    response = s3.get_object(Bucket=bucket, Key=key)
    lines = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(lines)
    
    cleaned_data = []
    for row in reader:
        if row.get('email') and '@' in row['email']:  # ejemplo de validación
            cleaned_data.append(row)

    # Guardar como JSON en bucket de salida
    output_bucket = os.environ['OUTPUT_BUCKET']
    output_key = key.replace('.csv', '.json')
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json.dumps(cleaned_data)
    )

    # Insertar en RDS
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pass, db=rds_db)
    with conn.cursor() as cur:
        for row in cleaned_data:
            cur.execute("INSERT INTO reportes (email, nombre) VALUES (%s, %s)", (row['email'], row['nombre']))
        conn.commit()
    conn.close()

    return {"statusCode": 200, "body": f"Procesado {len(cleaned_data)} registros"}
