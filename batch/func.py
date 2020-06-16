import io
import json
import os
import logging
import oci
import oci.object_storage

from fdk import response
import sys
sys.path.append('/function')

# config
# numero di colonne attese per vettore input
NUM_COLS = 12

def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("Costi-batch: vers. 1.0")
    
    signer = oci.auth.signers.get_resource_principals_signer()
    
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

    try:
        logging.getLogger().info("Costi-batch: Invoked...")

        # legge i dati dall'event
        body = json.loads(data.getvalue())
        resourceName = body["data"]["resourceName"]
        eventType = body["eventType"]
        source = body["source"]
        logging.info('***eventType: ' + eventType + ', resourceName: ' + resourceName)

        # il nome del file e' in resourceName
        namespace = os.environ.get("OCI_NAMESPACE")        
        bucket_name = os.environ.get("OCI_BUCKET")

        obj_file = client.get_object(namespace, bucket_name, resourceName)

        content = str(obj_file.data.content)

        logging.info('content: ' + content)        
        
    except Exception as ex:
        logging.getLogger().error("%s", str(ex))
    
    prediction = {}
    prediction['response'] = 'OK'

    return response.Response(
        ctx, response_data=json.dumps(prediction),
        headers={"Content-Type": "application/json"}
    )
