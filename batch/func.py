import io
import json
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
    
    try:
        logging.getLogger().info("Costi-batch: Invoked...")

        # legge i dati dall'event
        body = json.loads(data.getvalue())
        resourceName = body["data"]["resourceName"]
        eventType = body["eventType"]
        source = body["source"]
        logging.info('***eventType:' + eventType + ' resourceName:' + resourceName)

    except Exception as ex:
        logging.getLogger().error("%s", str(ex))
    
    prediction = {}
    prediction['response'] = 'OK'

    return response.Response(
        ctx, response_data=json.dumps(prediction),
        headers={"Content-Type": "application/json"}
    )
