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
    
    # 👇 ignorar filas vacías
    lines = [line for line in lines if line.strip() != '']
    
    reader = csv.DictReader(lines)
    raw_data = list(reader)

    # Subir todo tal cual a S3
    output_bucket = os.environ['OUTPUT_BUCKET']
    output_key = key.replace('.csv', '.json')
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json.dumps(raw_data, ensure_ascii=False)
    )

    # Insertar todos en RDS
    conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_pass, db=rds_db)
    with conn.cursor() as cur:
        campos_insertar = [
            "CODIGO", "CODIGO OEM", "MARCA AUTOMOVIL", "MODELO",
            "DESCRIPCION", "STOCK", "DIAMETRO", "MEDIDA", "AÑO", "MOTOR"
        ]
        sql = f"""
            INSERT INTO reportes ({', '.join(campos_insertar)})
            VALUES ({', '.join(['%s'] * len(campos_insertar))})
        """
        for row in raw_data:
            try:
                valores = [row.get(campo, '') for campo in campos_insertar]
                cur.execute(sql, valores)
            except Exception as e:
                print(f"❌ Error al insertar fila: {e}")
                continue
        conn.commit()
    conn.close()

    return {
        "statusCode": 200,
        "body": f"✅ Procesado {len(raw_data)} registros sin validación"
    }
