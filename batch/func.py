import io
import json
import os
import logging
import oci
import oci.object_storage
from io import StringIO
import pandas as pd

# per invocare il modello ML
import scorefn

from fdk import response
import sys
sys.path.append('/function')

# config
ENCODING = 'UTF-8'

# load ML model
model = scorefn.load_model()

# === Helper Functions ===

# numero di colonne attese per vettore input
NUM_COLS = 12

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

# la funzione deve eliminare dal dataframe tutte le colonne che non sono
# dati in input al modello
def trasforma_df(df):
    # anche questa va customizzata
    # in questo caso cancella le colonne non in input al modello
    cols_to_drop = ['anno', 'mese']

    for col in cols_to_drop:
        df = df.drop(col, axis=1)
    
    return df

# formatta il vettore di input per scrivere nel file di report
def formatta_input(vet):
    str_out = '['
    for index, val in enumerate(vet):
        str_out += str(round(val, 2))

        if index < NUM_COLS -1:
            str_out += ','
    
    str_out += ']'

    return str_out

# === Handler ===
def handler(ctx, data: io.BytesIO=None):
    # per il logging
    FORMAT = '%(asctime)-15s'
    logging.basicConfig(format=FORMAT)
    LOG = logging.getLogger('costi-model-batch')

    LOG.info(": vers. 1.5...")
    
    signer = oci.auth.signers.get_resource_principals_signer()
    
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

    # for sending notification (emails)
    notificationClient = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)

    result = {}
    result['response'] = 'OK'

    # legge i dati dall'Event
    body = json.loads(data.getvalue())
    resourceName = body["data"]["resourceName"]
    eventType = body["eventType"]

    # il nome del file e' in resourceName
    namespace = os.environ.get("OCI_NAMESPACE")        
    bucket_name = os.environ.get("OCI_BUCKET")
    base_name = resourceName.split('.')[0]

    try:
        # controlla che il file sia stato creato ed abbia estensione csv, solo i file csv sono elaborati
        if ("createobject" in eventType) and ("csv" in resourceName):

            # legge il contenuto del file
            obj_file = client.get_object(namespace, bucket_name, resourceName)

            content = obj_file.data.content.decode(ENCODING)

            # preparo il nome del file di report
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

                LOG.info(": Invoking predictions...")
                
                # *** invoco la predizione
                prediction = scorefn.predict(model, lista)

                LOG.info(": prediction %s", json.dumps(prediction))

                # prediction è un dictionary, estraggo la lista delle predizioni
                vet_prediction = prediction['prediction']

                # preparo il report
            
                for index, vet in enumerate(lista):
                    # vet è un vettore di 12 elementi, controllo già fatto
                    val_pred = round(vet_prediction[index], 2)
                    LOG.info(':riga: ' + formatta_input(vet) + ", " + str(val_pred))
                    # aggiungo riga al testo
                    report = report + "input: " + formatta_input(vet) + ", predizione: " + str(val_pred) + " mil. \n"
                    
                # produce il report
                my_data = report.encode(ENCODING)
                client.put_object(namespace, bucket_name, report_name, my_data, content_type='text/csv')

            else:
                LOG.info(', Input file non OK !')
                result['response'] = 'KO'

    except Exception as ex:
        LOG.error(", Errore in predictor_batch....")
        LOG.error("%s", str(ex))
        result['response'] = 'KO'
    
    if result['response'] == 'KO':
        # produce un file bad
        report = "Report relativo al file: " + resourceName + "\n\n"
        report += "elaborazioni interrotte..."

        my_data = report.encode(ENCODING)
        report_name = base_name + "_bad.txt"
        client.put_object(namespace, bucket_name, report_name, my_data, content_type='text/csv')

    return response.Response(
        ctx, response_data=json.dumps(result),
        headers={"Content-Type": "application/json"}
    )
