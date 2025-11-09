import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    body = event.get('body', '{}')
    if isinstance(body, str):
        body = json.loads(body)
    tenant_id = body.get('tenant_id', '')
    texto = body.get('texto', '')
    nombre_tabla = os.environ["TABLE_NAME"]
    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        }
    }
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    result = None
    ingest_bucket = os.environ.get('INGEST_BUCKET')
    if ingest_bucket:
        s3 = boto3.client('s3')
        key = f"{tenant_id}/{uuidv1}.json" if tenant_id else f"{uuidv1}.json"
        try:
            s3_resp = s3.put_object(Bucket=ingest_bucket, Key=key, Body=json.dumps(comentario), ContentType='application/json')
            result = {'bucket': ingest_bucket, 'key': key, 'etag': s3_resp.get('ETag')}
        except Exception as e:
            print('S3 upload error:', str(e))
            result = {'error': str(e)}
    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response,
        's3': result
    }
