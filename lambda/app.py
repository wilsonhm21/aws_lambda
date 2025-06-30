import boto3
import csv
import json
import os
import re
from datetime import datetime

s3 = boto3.client('s3')

# Lista de valores inválidos conocidos
placeholders = {'n/a', '-', 'null', 'sin dato', 'none', ''}

# Año actual
current_year = datetime.now().year

required_fields = ["CODIGO", "CODIGO OEM", "MARCA AUTOMOVIL", "MODELO", "DESCRIPCION", "STOCK"]

def is_valid_row(row, header):
    if len(row) != len(header):
        return False

    if not row.get("MARCA AUTOMOVIL", "").isalpha():
        return False

    if any(',' in str(v) and str(v).count(',') > 1 for v in row.values()):
        return False

    for field in required_fields:
        if row.get(field,"").strip().lower() in placeholders:
            return False

    empty_count = sum(1 for v in row.values() if str(v).strip().lower() in placeholders)
    if empty_count / len(row) > 0.3:
        return False

    if any(str(v).strip().lower() in placeholders for v in row.values()):
        return False

    try:
        stock = int(row.get("STOCK",-1))
        if stock < 0 or stock > 10000:
            return False
    except:
        return False

    diametro = row.get("DIAMETRO","")
    if not re.match(r"^\d{2,3}mm$", diametro):
        return False
    try:
        diametro_val = int(diametro.replace("mm",""))
        if not 30 <= diametro_val <= 120:
            return False
    except:
        return False

    medida = row.get("MEDIDA","")
    if not re.match(r"^\d+(\.\d+)?\*\d+(\.\d+)?\*\d+(\.\d+)?$", medida):
        return False
    try:
        partes = [float(x) for x in medida.split("*")]
        if any(p<=0.1 or p>50 for p in partes):
            return False
    except:
        return False

    anio = row.get("AÑO")
    if anio:
        try:
            anio_int = int(anio)
            if anio_int < 1950 or anio_int > current_year:
                return False
        except:
            return False

    descripcion = row.get("DESCRIPCION","").strip()
    if len(descripcion) < 5 or len(descripcion.split()) < 2:
        return False

    if any(re.search(r"[@#?%&]", str(v)) for v in row.values()):
        return False

    if row.get("MARCA AUTOMOVIL","").isdigit():
        return False

    for k,v in row.items():
        if v.islower() or v.isupper():
            continue
        elif not re.match(r'^[A-Z][a-z]',v):
            return False

    if any(re.match(r"^\d+$", str(row.get(f,""))) and f in ["MARCA AUTOMOVIL","DESCRIPCION"] for f in row):
        return False

    return True

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']

    response = s3.get_object(Bucket=bucket, Key=key)
    lines = response['Body'].read().decode('utf-8').splitlines()
    lines = [line for line in lines if line.strip() != '']

    reader = csv.DictReader(lines)
    header = reader.fieldnames
    raw_data = list(reader)

    # deduplicado exacto
    seen_rows = set()
    unique_data = []
    for row in raw_data:
        row_tuple = tuple(row.items())
        if row_tuple not in seen_rows:
            seen_rows.add(row_tuple)
            unique_data.append(row)

    # deduplicado clave
    key_seen = set()
    cleaned_data = []
    for row in unique_data:
        combo = (row.get("CODIGO"),row.get("CODIGO OEM"),row.get("MOTOR"),row.get("MEDIDA"),row.get("STOCK"))
        if combo not in key_seen and is_valid_row(row, header):
            key_seen.add(combo)
            cleaned_data.append(row)

    output_bucket = os.environ['OUTPUT_BUCKET']
    output_key = key.replace(".csv",".json")
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json.dumps(cleaned_data, ensure_ascii=False)
    )

    return {
        "statusCode":200,
        "body": f"✅ JSON limpio guardado en {output_bucket}/{output_key} con {len(cleaned_data)} registros"
    }
