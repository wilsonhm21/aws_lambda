import boto3
import csv
import json
import os
import re
from datetime import datetime

s3 = boto3.client('s3')

# Configuración de validación
PLACEHOLDERS = {'n/a', '-', 'null', 'sin dato', 'none', '', 'na'}
REQUIRED_FIELDS = ["codigo", "nombre", "descripcion", "marca", "stock", "precio"]
MARCAS_VALIDAS = {'NGK', 'Denso', 'Delphi', 'ACDelco', 'Bosch', 'Valeo', 'Magneti Marelli', 'Hella'}

def validar_codigo(codigo):
    """Valida el formato del código COD-XXXXXX"""
    return bool(re.fullmatch(r'^COD-[a-zA-Z0-9]{6,8}$', codigo.strip()))

def validar_marca(marca):
    """Valida que la marca esté en la lista de marcas válidas"""
    return marca.strip() in MARCAS_VALIDAS

def validar_stock(stock):
    """Valida que el stock sea un entero positivo"""
    try:
        return 0 <= int(stock) <= 1000
    except (ValueError, TypeError):
        return False

def validar_precio(precio):
    """Valida que el precio sea un decimal positivo"""
    try:
        return 0 < float(precio) <= 10000
    except (ValueError, TypeError):
        return False

def validar_descripcion(descripcion):
    """Valida la estructura de la descripción"""
    desc = descripcion.strip()
    return (len(desc) >= 5 and 
            len(desc.split()) >= 2 and 
            not desc.startswith(('Participant', 'House', 'Approach')))

def es_valido(row):
    """Aplica todas las validaciones a un registro"""
    # Validación de campos requeridos
    if any(field not in row or str(row[field]).strip().lower() in PLACEHOLDERS 
           for field in REQUIRED_FIELDS):
        return False
    
    # Validaciones específicas por campo
    validations = [
        validar_codigo(row['codigo']),
        validar_marca(row['marca']),
        validar_stock(row['stock']),
        validar_precio(row['precio']),
        validar_descripcion(row['descripcion'])
    ]
    
    return all(validations)

def limpiar_datos(row):
    """Normaliza y limpia los datos del registro"""
    cleaned = {k.lower().strip(): str(v).strip() 
              for k, v in row.items() if str(v).strip()}
    
    # Conversión de tipos
    try:
        cleaned['stock'] = int(cleaned['stock'])
        cleaned['precio'] = float(cleaned['precio'])
    except (ValueError, TypeError):
        pass
        
    return cleaned

def lambda_handler(event, context):
    # Configuración S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Leer archivo CSV
        response = s3.get_object(Bucket=bucket, Key=key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        
        # Procesar CSV
        reader = csv.DictReader(lines)
        raw_data = [limpiar_datos(row) for row in reader if any(field.strip() for field in row.values())]
        
        # Deduplicación
        unique_data = []
        seen_codes = set()
        
        for row in raw_data:
            if row['codigo'] not in seen_codes and es_valido(row):
                seen_codes.add(row['codigo'])
                unique_data.append(row)
        
        # Generar salida
        output_bucket = os.environ['OUTPUT_BUCKET']
        output_key = f"validated_{os.path.basename(key)}.json"
        
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=json.dumps(unique_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        # Estadísticas
        stats = {
            'archivo_origen': key,
            'registros_leidos': len(raw_data),
            'registros_validos': len(unique_data),
            'registros_invalidos': len(raw_data) - len(unique_data),
            'fecha_procesamiento': datetime.now().isoformat()
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Procesamiento completado exitosamente',
                'estadisticas': stats,
                'ubicacion_resultados': f"s3://{output_bucket}/{output_key}"
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Error en el procesamiento del archivo'
            })
        }