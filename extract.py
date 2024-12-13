#Importing Dependencies

import requests
import json
import time
from datetime import datetime
import psycopg2
from dotenv import load_dotenv 
import os
import logging


#Extracting the data from the api

def main(**kwargs):
    app_id = os.getenv('APP_ID'),
    APi_key = os.getenv('API_KEY'),
    POLL_INTERVAL = 60 

    # Define the endpoint and parameters for scheduled data
    station_code = 'WAT'  # station code for London Waterloo station
    url = f'https://transportapi.com/v3/uk/train/station/{station_code}/live.json'

    params = {
        'app_id': app_id,
        'api_key': APi_key,
        'darwin': 'false',  
        'train_status': 'passenger', 
        'live' :'True'
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        response= response.json()
        print("Data Extracted successfully")

    else:
        print(f"Failed to fetch data: {response.status_code}")

    
    logging.info("kwargs: %s", kwargs)

    kwargs['ti'].xcom_push(key="raw_data", value=response)
    print("Data pushed to xcom")

    return response


if __name__ == "__main__":
    main()
