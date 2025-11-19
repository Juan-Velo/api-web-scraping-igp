import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import os
import logging

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # URL actualizada que contiene la tabla estática (la principal es SPA)
    url = "https://www.igp.gob.pe/servicios/centro-sismologico-nacional/ultimo-sismo/sismos-reportados"
    table_name = os.environ.get('TABLE_NAME', 'TablaSismosIGP')
    
    # 1. Realizar la solicitud HTTP (Simulamos ser un navegador para evitar bloqueos simples)
    headers_agent = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers_agent, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Error al conectar con IGP: {e}")
        return {'statusCode': 500, 'body': f'Error conectando a IGP: {str(e)}'}

    # 2. Parsear HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # En el sitio del IGP, la tabla suele tener la clase 'table' o estar dentro de un contenedor específico
    # Buscamos la tabla que contiene "Fecha y hora" en sus encabezados o datos típicos
    target_table = None
    tables = soup.find_all('table')
    
    for t in tables:
        # Buscamos "Magnitud" en el texto de la tabla O un patrón de datos conocido como "IGP/CENSIS"
        if "Magnitud" in t.text or "IGP/CENSIS" in t.text:
            target_table = t
            break
            
    if not target_table:
        logger.error("No se encontró la tabla de sismos en el HTML")
        return {'statusCode': 404, 'body': 'Estructura de web IGP cambió, tabla no encontrada.'}

    # 3. Extraer filas (Limitamos a 10)
    sismos = []
    # El tbody suele contener los datos
    tbody = target_table.find('tbody')
    rows = tbody.find_all('tr') if tbody else target_table.find_all('tr')[1:]
    
    # Iterar y extraer datos
    for row in rows[:10]: # Solo los 10 primeros
        cells = row.find_all('td')
        if len(cells) >= 4: # Asegurar que la fila tenga datos
            # Estructura usual IGP: [Reporte, Fecha, Referencia, Magnitud, ...]
            # A veces varía, ajustamos basado en la posición visual:
            # Col 0: Fecha/Hora | Col 1: Referencia (Lugar) | Col 2: Magnitud 
            # (Nota: Esto depende de la renderización exacta, a veces IGP pone primero el ID)
            
            # Basado en la inspección visual típica de la tabla IGP:
            # Columna 0: Enlace/ID, Columna 1: Referencia, Columna 2: Fecha, Columna 3: Magnitud
            # Vamos a intentar extraer texto limpio
            
            data = {
                'id': str(uuid.uuid4()),
                'fecha_local': cells[2].get_text(strip=True),
                'ubicacion': cells[1].get_text(strip=True),
                'magnitud': cells[3].get_text(strip=True),
                'reporte_origen': cells[0].get_text(strip=True)
            }
            sismos.append(data)

    # 4. Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        # A. Eliminar datos antiguos (Scan + Batch Delete)
        # Nota: En producción con muchos datos, scan es costoso. Para 10 items está bien.
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan.get('Items', []):
                batch.delete_item(Key={'id': each['id']})

        # B. Insertar nuevos datos
        with table.batch_writer() as batch:
            for sismo in sismos:
                batch.put_item(Item=sismo)
                
        return {
            'statusCode': 200,
            'body': {
                'message': 'Scraping exitoso',
                'cantidad': len(sismos),
                'data': sismos
            }
        }
        
    except Exception as e:
        logger.error(f"Error DB: {e}")
        return {'statusCode': 500, 'body': f'Error guardando en DynamoDB: {str(e)}'}