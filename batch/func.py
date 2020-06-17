import io
import json
import os
import logging
import oci
import oci.object_storage
from oci import functions
from oci import identity
from oci import pagination
from io import StringIO
import pandas as pd

# per invocare il modello ML
import scorefn

from fdk import response
import sys
sys.path.append('/function')

# config
# numero di colonne attese per vettore input
ENCODING = 'UTF-8'
NUM_COLS = 12
model = scorefn.load_model()

# === Handler ===
def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("Costi-batch: vers. 1.0...")
    
    signer = oci.auth.signers.get_resource_principals_signer()
    
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

    try:
        # legge i dati dall'event
        body = json.loads(data.getvalue())
        resourceName = body["data"]["resourceName"]
        eventType = body["eventType"]
        
        # controlla che il file abbia estensione csv, solo i file csv sono elaborati
        if "csv" in resourceName:

            logging.info('***eventType: ' + eventType + ', resourceName: ' + resourceName)

            # il nome del file e' in resourceName
            # compartment è il compartment della function ML da invocare
            namespace = os.environ.get("OCI_NAMESPACE")        
            bucket_name = os.environ.get("OCI_BUCKET")

            # legge il contenuto del file
            obj_file = client.get_object(namespace, bucket_name, resourceName)

            content = obj_file.data.content.decode(ENCODING)

            # preparo il nome del file di report
            base_name = resourceName.split('.')[0]
            report_name = base_name + "_report.txt"

            # uso pandas per il parsing del csv
            df = pd.read_csv(StringIO(content))

            # elimino le colonne anno e mese che non sono usate dal modello
            df = df.drop('anno', axis=1)
            df = df.drop('mese', axis=1)

            # in questo modo ho una lista di liste
            lista = df.values

            report = "Report relativo al file: " + resourceName + "\n\n"

            # invoco la predizione
            logging.getLogger().info("Costi-model: Invoked...")

            prediction = scorefn.predict(model, lista)

            logging.getLogger().info("Costi-model: prediction %s", json.dumps(prediction))

            # prediction è un dictionary, estraggo la lista delle predizioni
            vet_prediction = prediction['prediction']

            # preparo il report
            
            for index, vet in enumerate(lista):
                # vet è un vettore di 12 elementi
                if vet.shape[0] == NUM_COLS:
                    val_pred = round(vet_prediction[index], 2)
                    logging.info('riga: ' + str(vet) + ", " + str(val_pred))
                    # aggiungo riga al testo
                    report = report + "input: " + str(vet) + ", predizione: " + str(val_pred) + "\n"
                    
            # produce il report
            my_data = report.encode(ENCODING)

            client.put_object(namespace, bucket_name, report_name, my_data, content_type='text/csv')

    except Exception as ex:
        logging.getLogger().error("%s", str(ex))
    
    result = {}
    result['response'] = 'OK'

    return response.Response(
        ctx, response_data=json.dumps(result),
        headers={"Content-Type": "application/json"}
    )
