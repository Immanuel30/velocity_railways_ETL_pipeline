import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv 
import os
import pandas as pd

#Loading data into azure postgresql database

# Defining database connection parameters

def azure_load(**kwargs):

    #Reading the data

    ti = kwargs['ti']
    train_schedule_df =ti.xcom_pull(key="transformed_data", task_ids="transformation_layer")

    conn_params = {
        "host": "postgres-server123.postgres.database.azure.com",
        "database": "train_schedule",
        "user": "adminuser",
        "password": "Password1",
        "port": "5432",
        "sslmode": "require"
    }

    # Connect to the database
    try:
        azure_conn = psycopg2.connect(**conn_params)
        print("Connection successful")
    except Exception as e:
        print(f"Error connecting to database: {e}")


    azure_cursor = azure_conn.cursor()

    #Create table

    #Execute query to create table
    azure_cursor.execute("""CREATE TABLE IF NOT EXISTS train_schedule(
        id SERIAL PRIMARY KEY,
        request_date_time  VARCHAR (100),
        station_name VARCHAR (100),
        mode VARCHAR (100),
        train_uid VARCHAR (100),
        origin_name VARCHAR (100),
        operator_name VARCHAR (100),
        platform VARCHAR(100),
        destination_name VARCHAR(100),
        aimed_departure_time VARCHAR (100),
        expected_departure_time VARCHAR(100),
        best_departure_estimate_mins VARCHAR(100),
        aimed_arrival_time VARCHAR (100)

    );
    """)

    #committing the query to database
    azure_conn.commit()


    # Data loading function to azure db
    def load_data_to_azure(azure_conn, train_schedule_df, train_schedule):
        azure_cursor = azure_conn.cursor()
        
        # Define the SQL insert query with correct syntax
        insert_query = f'''
        INSERT INTO {train_schedule} (
            request_date_time, station_name, mode, train_uid, origin_name, operator_name, platform, 
            destination_name, aimed_departure_time, expected_departure_time, best_departure_estimate_mins, aimed_arrival_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        # Define rows by selecting individual columns in the correct format
        rows = [tuple(x) for x in train_schedule_df[[
            "request_date_time", "station_name", "mode", "train_uid", "origin_name",
            "operator_name", "platform", "destination_name", "aimed_departure_time",
            "expected_departure_time", "best_departure_estimate_mins", "aimed_arrival_time"
        ]].values]
        
        try:
            azure_cursor.executemany(insert_query, rows)
            azure_conn.commit()
            print(f"Data loaded successfully into {train_schedule}")
        except Exception as e:
            azure_conn.rollback()
            print(f"Error loading data into {train_schedule}: {e}")
        finally:
            azure_cursor.close()

    # Define DataFrame and load data
    train_schedule = 'train_schedule'
    # Ensure 'train_schedule_df' is the DataFrame you want to load
    load_data_to_azure(azure_conn, train_schedule_df, train_schedule)

    # Close connections
    azure_conn.close()

if __name__ == "__main__":
    azure_load()