import io
import json
import logging

from fdk import response
import sys
sys.path.append('/function')
import scorefn

model = scorefn.load_model()

def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("Costi-model: vers. 1.1")
    
    try:
        input = json.loads(data.getvalue())['input']

        # logga l'input, per poter controllare
        input_json = json.dumps(input)
        logging.getLogger().info("Costi-model: Invoked with input %s", input_json)

        prediction = scorefn.predict(model, input)
    except (Exception, ValueError) as ex:
        logging.getLogger().error("%s", str(ex))

    logging.getLogger().info("Costi-model: prediction %s", json.dumps(prediction))

    return response.Response(
        ctx, response_data=json.dumps(prediction),
        headers={"Content-Type": "application/json"}
    )