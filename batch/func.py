import io
import json
import logging

from fdk import response
import sys
sys.path.append('/function')
import scorefn

# config
# numero di colonne attese per vettore input
NUM_COLS = 12

def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("Costi-batch: vers. 1.0")
    
    try:
        logging.getLogger().info("Costi-batch: Invoked...")

    except (Exception, ValueError) as ex:
        logging.getLogger().error("%s", str(ex))
    
    prediction = {}
    prediction['response'] = 'OK'

    return response.Response(
        ctx, response_data=json.dumps(prediction),
        headers={"Content-Type": "application/json"}
    )
