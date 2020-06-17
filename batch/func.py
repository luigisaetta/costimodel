import io
import json
import os
import logging
import oci
import oci.object_storage
from oci import functions
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

# load ML model
model = scorefn.load_model()

# === Helper Functions ===
def check_contents(df):
    # la funzione assume che il file possa essere letto in un dataframe
    # ritorna true se il dataframe è OK...
    # da customizzare
    isOK = True

    lista = df.values

    # controlla tutte le righe
    for vet in lista:
        if vet.shape[0] != NUM_COLS:
            isOK = False

    return isOK

def trasforma_df(df):
    # anche questa va customizzata
    df = df.drop('anno', axis=1)
    df = df.drop('mese', axis=1)

    return df

def formatta_input(vet):
    # formatta il vettore di input per scrivere nel report
    str_out = '['
    for index, val in enumerate(vet):
        str_out += str(round(val, 2))

        if index < NUM_COLS:
            str_out += ','
    
    str_out += ']'

    return str_out

# === Handler ===
def handler(ctx, data: io.BytesIO=None):
    logging.getLogger().info("Costi-batch: vers. 1.0...")
    
    signer = oci.auth.signers.get_resource_principals_signer()
    
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

    result = {}
    result['response'] = 'OK'

    try:
        # legge i dati dall'event
        body = json.loads(data.getvalue())
        resourceName = body["data"]["resourceName"]
        eventType = body["eventType"]
        
        # controlla che il file abbia estensione csv, solo i file csv sono elaborati
        if "csv" in resourceName:

            logging.info('***eventType: ' + eventType + ', resourceName: ' + resourceName)

            # il nome del file e' in resourceName
            namespace = os.environ.get("OCI_NAMESPACE")        
            bucket_name = os.environ.get("OCI_BUCKET")

            # legge il contenuto del file
            obj_file = client.get_object(namespace, bucket_name, resourceName)

            content = obj_file.data.content.decode(ENCODING)

            # preparo il nome del file di report
            base_name = resourceName.split('.')[0]
            report_name = base_name + "_report.txt"

            # uso Pandas per il parsing del csv
            df = pd.read_csv(StringIO(content))

            # elimino le colonne (anno e mese) che non sono usate dal modello
            df = trasforma_df(df)

            # alcuni controlli sui dati in input
            if check_contents(df):
                # in questo modo ho una lista di liste
                lista = df.values

                # prima riga del report
                report = "Report relativo al file: " + resourceName + "\n\n"

                logging.getLogger().info("Costi-model: Invoked...")
                # invoco la predizione
                prediction = scorefn.predict(model, lista)

                logging.getLogger().info("Costi-model: prediction %s", json.dumps(prediction))

                # prediction è un dictionary, estraggo la lista delle predizioni
                vet_prediction = prediction['prediction']

                # preparo il report
            
                for index, vet in enumerate(lista):
                    # vet è un vettore di 12 elementi, controllo già fatto
                    val_pred = round(vet_prediction[index], 2)
                    logging.info('riga: ' + formatta_input(vet) + ", " + str(val_pred))
                    # aggiungo riga al testo
                    report = report + "input: " + formatta_input(vet) + ", predizione: " + str(val_pred) + "\n"
                    
                # produce il report
                my_data = report.encode(ENCODING)

                client.put_object(namespace, bucket_name, report_name, my_data, content_type='text/csv')
            else:
                logging.info('Input file non OK !')
                result['response'] = 'KO'

    except Exception as ex:
        logging.getLogger().error("Errore in predictor_batch....")
        logging.getLogger().error("%s", str(ex))
        result['response'] = 'KO'
    
    return response.Response(
        ctx, response_data=json.dumps(result),
        headers={"Content-Type": "application/json"}
    )
