import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv 
import os
import pandas as pd 

def postgres_load():

    #Readdin the data
    train_schedule_df = pd.read_csv(r'train_schedule.csv')
    # Database connection details

    local_conn = psycopg2.connect(
                            host= 'localhost', 
                            database = 'velocity_railway', 
                            user = 'postgres', 
                            password = 'Password', 
                            port= '5432'
    )

    # Connect to Azure and Local PostgreSQL
    #azure_conn = psycopg2.connect(**azure_conn_details)
    #local_conn = psycopg2.connect(local_conn_details)

    cursor = local_conn.cursor()

    #Create table

    #Execute query to create table
    cursor.execute("""CREATE TABLE IF NOT EXISTS train_schedule(
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
        best_departure_estimate_mins INT,
        aimed_arrival_time VARCHAR (100)

    );
    """)


    #committing the query to database
    local_conn.commit()


    # Data loading function
    def load_data_to_db(local_conn, train_schedule_df, train_schedule):
        cursor = local_conn.cursor()
        
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
            cursor.executemany(insert_query, rows)
            local_conn.commit()
            print(f"Data loaded successfully into {train_schedule}")
        except Exception as e:
            local_conn.rollback()
            print(f"Error loading data into {train_schedule}: {e}")
        finally:
            cursor.close()

    # Define DataFrame and load data
    train_schedule = 'train_schedule'
    # Ensure 'train_schedule_df' is the DataFrame you want to load
    load_data_to_db(local_conn, train_schedule_df, train_schedule)

    # Close connections
    local_conn.close()


if __name__ == "__main__":
    postgres_load()

