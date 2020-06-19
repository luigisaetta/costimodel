import io
import json
import os
import logging
from datetime import datetime, timedelta
import oci
import oci.object_storage


from fdk import response
import sys
sys.path.append('/function')

# config
ENCODING = 'UTF-8'


# === Helper Functions ===
def build_bodyMail(file_name, par_url, exp_date):
    bodyMessage = "Il Machine Learning report: " + file_name + " è stato generato !! \n\n"
    bodyMessage += "Puoi scaricarlo al link: " + par_url + "\n\n"
    # elimino i secondi finali
    bodyMessage += "Il download è possibile fino alla data: " + str(exp_date)[:-10]

    return bodyMessage

# === Handler ===

# this is func2 !

def handler(ctx, data: io.BytesIO=None):
    # per il logging
    FORMAT = '%(asctime)-15s'
    logging.basicConfig(format=FORMAT)
    LOG = logging.getLogger('sender_notification')

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
    topic_ocid = os.environ.get("OCI_TOPIC_OCID")
    
    # base_name = resourceName.split('.')[0]

    try:
        # controlla che il file sia stato creato ed abbia estensione txt, solo i file txt sono elaborati
        if ("createobject" in eventType) and ("txt" in resourceName):

            # deve solo inviare notifica

            # genera la pre-authenticated request per accedere al report su Object Storage

            # durata max della par (+1 giorno)
            d = datetime.utcnow() + timedelta(hours=24)

            par_req_detail = oci.object_storage.models.CreatePreauthenticatedRequestDetails(name = "par_req",
            access_type = 'ObjectRead', object_name = resourceName,  time_expires=d)
                
            par_resp = client.create_preauthenticated_request(namespace, bucket_name, par_req_detail)
                
            # costruisce la url della PAR
            url = "https://objectstorage.eu-frankfurt-1.oraclecloud.com" + par_resp.data.access_uri
            LOG.info(url)

            # costruisce il body della mail
            bodyMessage = build_bodyMail(resourceName, url, d)
                
            notificationMessage = {"default": "MLMsg", "body": bodyMessage, "title": "ML report generato"}
                
            LOG.info("invia notifica...")
                
            notificationClient.publish_message(topic_ocid, notificationMessage)

    except Exception as ex:
        LOG.error(", Errore in sender_notification....")
        LOG.error("%s", str(ex))
        result['response'] = 'KO'
    

    return response.Response(
        ctx, response_data=json.dumps(result),
        headers={"Content-Type": "application/json"}
    )
