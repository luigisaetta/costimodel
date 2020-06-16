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
            compartment = os.environ.get("OCI_COMPARTMENT")
            namespace = os.environ.get("OCI_NAMESPACE")        
            bucket_name = os.environ.get("OCI_BUCKET")

            # legge il contenuto del file
            obj_file = client.get_object(namespace, bucket_name, resourceName)

            content = obj_file.data.content.decode('UTF-8')

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

            report = "Report relativo al file: " + resourceName + "\n"

            # invoco la predizione
            logging.getLogger().info("Costi-model: Invoked...")

            prediction = scorefn.predict(model, lista)

            logging.getLogger().info("Costi-model: prediction %s", json.dumps(prediction))

            index = 0
            for vet in lista:
                # vet è un vettore di 12 elementi
                if vet.shape[0] == 12:
                    logging.info('riga: ' + str(vet))

                    report = report + "input: " + str(vet) + ", predizione: " + str(prediction[index] + "\n"
                    index += 1


            # produce il report
            my_data = report.encode('UTF-8')

            client.put_object(namespace, bucket_name, report_name, my_data, content_type='text/csv')

    except Exception as ex:
        logging.getLogger().error("%s", str(ex))
    
    prediction = {}
    prediction['response'] = 'OK'

    return response.Response(
        ctx, response_data=json.dumps(prediction),
        headers={"Content-Type": "application/json"}
    )
