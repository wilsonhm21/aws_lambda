import boto3
import csv
import json
import pymysql
import os
import re
from datetime import datetime

s3 = boto3.client('s3')

# Configuración para RDS
rds_host = os.environ['RDS_HOST']
rds_user = os.environ['RDS_USER']
rds_pass = os.environ['RDS_PASS']
rds_db   = os.environ['RDS_DB']

# Lista de valores inválidos conocidos
placeholders = {'n/a', '-', 'null', 'sin dato', 'none', ''}

# AÑO actual para validación
current_year = datetime.now().year

# Campos obligatorios (por nombre en encabezado)
required_fields = ["CODIGO", "CODIGO OEM", "MARCA AUTOMOVIL", "MODELO", "DESCRIPCION", "STOCK"]

def is_valid_row(row, header):
    # Regla 1: Cantidad de columnas
    if len(row) != len(header):
        return False

    # Regla 2: Campos desfasados (ej: número en campo de texto)
    if not row.get("MARCA AUTOMOVIL", "").isalpha():
        return False

    # Regla 3: Delimitadores fallidos
    if any(',' in str(v) and str(v).count(',') > 1 for v in row.values()):
        return False

    # Regla 4: Campos obligatorios vacíos
    for field in required_fields:
        value = row.get(field, "").strip().lower()
        if value in placeholders:
            return False

    # Regla 5: Más del 30% vacío
    empty_count = sum(1 for v in row.values() if str(v).strip().lower() in placeholders)
    if empty_count / len(row) > 0.3:
        return False

    # Regla 6: Texto de marcador de posición
    if any(str(v).strip().lower() in placeholders for v in row.values()):
        return False

    # Regla 7: STOCK debe ser entero positivo
    try:
        stock = int(row.get("STOCK", -1))
        if stock < 0:
            return False
    except:
        return False

    # Regla 8: DIAMETRO con "mm"
    diametro = row.get("DIAMETRO", "")
    if not re.match(r"^\d{2,3}mm$", diametro):
        return False

    # Regla 9: MEDIDA patrón n.n*n.n*n.n
    medida = row.get("MEDIDA", "")
    if not re.match(r"^\d+(\.\d+)?\*\d+(\.\d+)?\*\d+(\.\d+)?$", medida):
        return False

    # Regla 10: AÑO debe ser entre 1950 y actual
    anio = row.get("AÑO")
    if anio:
        try:
            anio_int = int(anio)
            if anio_int < 1950 or anio_int > current_year:
                return False
        except:
            return False

    # Regla 11: DESCRIPCION con mínimo 5 caracteres y más de una palabra
    descripcion = row.get("DESCRIPCION", "").strip()
    if len(descripcion) < 5 or len(descripcion.split()) < 2:
        return False

    # Regla 12: Caracteres extraños
    if any(re.search(r"[@#?%&]", str(v)) for v in row.values()):
        return False

    # Regla 13: Campos alfanuméricos solo con números
    if row.get("MARCA AUTOMOVIL", "").isdigit():
        return False

    # Regla 14: Texto completamente en mayúscula o minúscula inconsistente
    for k, v in row.items():
        if v.islower() or v.isupper():
            continue
        elif not re.match(r'^[A-Z][a-z]', v):
            return False

    # Regla 15: Duplicados exactos se eliminarán después
    # Regla 16: Duplicados por clave también después

    # Regla 17: STOCK no mayor a 10,000
    if stock > 10000:
        return False

    # Regla 18: DIAMETRO entre 30 y 120 mm
    try:
        diametro_val = int(diametro.replace("mm", ""))
        if not (30 <= diametro_val <= 120):
            return False
    except:
        return False

    # Regla 19: MEDIDA con valores proporcionales razonables
    try:
        partes = [float(p) for p in medida.split("*")]
        if any(p <= 0.1 or p > 50 for p in partes):
            return False
    except:
        return False

    # Regla 20: Validación general de tipo
    if any(re.match(r"^\d+$", str(row.get(f, ""))) and f in ["MARCA AUTOMOVIL", "DESCRIPCION"] for f in row):
        return False

    return True

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']

    response = s3.get_object(Bucket=bucket, Key=key)
    lines = response['Body'].read().decode('utf-8').splitlines()

        # 👇 Regla agregada para ignorar filas vacías
    lines = [line for line in lines if line.strip() != '']
    reader = csv.DictReader(lines)

    header = reader.fieldnames
    raw_data = list(reader)

    # Limpiar duplicados exactos (Regla 15)
    seen_rows = set()
    unique_data = []
    for row in raw_data:
        row_tuple = tuple(row.items())
        if row_tuple not in seen_rows:
            seen_rows.add(row_tuple)
            unique_data.append(row)

    # Eliminar duplicados por combinación clave (Regla 16)
    key_seen = set()
    cleaned_data = []
    for row in unique_data:
        combo_key = (
            row.get("CODIGO"),
            row.get("CODIGO OEM"),
            row.get("MOTOR"),
            row.get("MEDIDA"),
            row.get("STOCK")
        )
        if combo_key not in key_seen and is_valid_row(row, header):
            key_seen.add(combo_key)
            cleaned_data.append(row)

    # Guardar como JSON en bucket de salidaa
    output_bucket = os.environ['OUTPUT_BUCKET']
    output_key = key.replace('.csv', '.json')
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json.dumps(cleaned_data, ensure_ascii=False)
    )

    # Insertar en RDS
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

        for row in cleaned_data:
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
        "body": f"✅ Procesado {len(cleaned_data)} registros válidos"}
