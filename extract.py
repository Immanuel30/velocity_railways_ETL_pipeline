#Importing Dependencies

import requests
import json
import time
from datetime import datetime
import pickle
import psycopg2
from dotenv import load_dotenv 
import os


#Extracting the data from the api

def main():
    app_id = 'e2e4d023'
    #os.getenv('APP_ID'),
    APi_key = 'bf7baff0c295d4fc1ab3e121837d3d68'
    #os.getenv('API_KEY'),
    POLL_INTERVAL = 60  # Time in seconds between requests

    # Define the endpoint and parameters for scheduled data
    station_code = 'WAT'  # Example station code (London Waterloo)
    url = f'https://transportapi.com/v3/uk/train/station/{station_code}/live.json'

    params = {
        'app_id': app_id,
        'api_key': APi_key,
        #'time_of_day': '19:00',
        #'request_time': '2024-10-30T18:50:00+00:00',
        'darwin': 'false',  
        'train_status': 'passenger',  # Status filter, e.g., passenger trains only
        'live' :'True',
        #'station_detail': 'destination'
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        response= response.json()
        print("Data Extracted successfully")
    else:
        print(f"Failed to fetch data: {response.status_code}")


if __name__ == "__main__":
    main()
