import pandas as pd
import json
import time
from datetime import datetime

#reading the data
response= response.json()

def transformation():

    #Filtering relevant columns
    train_columns = []

    #date = response['date']
    request_time = response['request_time']
    station_name = response['station_name']

    train_info = {
        #'date': date,
        'request_time': request_time,
        'station_name': station_name
    }
    train_columns.append(train_info)

    train_columns

    #Passing the filtered data into a dataframe
    train_columns_df = pd.DataFrame(train_columns)
    train_columns_df.reset_index(inplace=True)
    train_columns_df