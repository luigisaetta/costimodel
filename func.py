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
model = scorefn.load_model()

def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("Costi-model: vers. 1.2")
    
    try:
        input = json.loads(data.getvalue())['input']

        # logga l'input, per poter controllare
        input_json = json.dumps(input)
        logging.getLogger().info("Costi-model: Invoked with input %s", input_json)

        # check num columns
        num_cols = len(input[0])

        # attende 12 colonne per vettore
        if num_cols != NUM_COLS:
            logging.getLogger().error("Costi-model: num_cols in input not OK.")
        else:
            prediction = scorefn.predict(model, input)

            logging.getLogger().info("Costi-model: prediction %s", json.dumps(prediction))

    except (Exception, ValueError) as ex:
        logging.getLogger().error("%s", str(ex))

    return response.Response(
        ctx, response_data=json.dumps(prediction),
        headers={"Content-Type": "application/json"}
    )