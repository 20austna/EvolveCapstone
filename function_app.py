import logging
import os
import azure.functions as func
import requests
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import timedelta, datetime

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 10 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    load_dotenv()
    API_KEY = os.getenv("LASTFM_API_KEY")
    LAST_FM_URL = "http://ws.audioscrobbler.com/2.0/"
    response = requests.get(f"{LAST_FM_URL}?method=chart.gettoptracks&api_key={API_KEY}&format=json")
    if response.status_code != 200:
        logging.error(f"API request failed: {response.status_code}, {response.text}")
        return
    
    val = response.json()

    #Initialize the output filename
    output_file = '/tmp/last_fm_data.json'
    with open(output_file, 'w') as file:
        json.dump(val, file, indent=4)


    current_datetime = datetime.now()
    previous_datetime = current_datetime - timedelta(days=1)
    #PUT YOUR NAME HERE
    YourName = 'Nathaniel'
    
    #Initialize a connection string and the name of the file you want to upload, along with the container you want to upload the file to
<<<<<<< HEAD
    connection_string = os.getenv("CONNECT_STRING")
=======
    connection_string = os.getenv("CON_STRING")
>>>>>>> f1e032e (Initial commit)
    blob_name = f"{previous_datetime.strftime('%m%d%Y')}{YourName}Data.json"
    container_name = 'data-ingestion-capstone'
    #Upload the file to the blob storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    with open(output_file, 'rb') as data:
        container_client.upload_blob(blob_name, data)

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
