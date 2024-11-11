import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv 
import os
import pandas as pd


def postgres_load(**kwargs):

    ti = kwargs['ti']
    train_schedule_df =ti.xcom_pull(key="transformed_data", task_ids="transformation_layer")
    
    #Readdin the data
    #train_schedule_df = pd.read_csv(r'/mnt/c/Users/imarr/OneDrive/Desktop/DE/Velocity_railway/train_schedule.csv')
    #train_schedule_df = pd.read_csv(r'C:\Users\imarr\OneDrive\Desktop\DE\Velocity_railway\train_schedule.csv')
    # Database connection details

    windows_ip = '192.168.0.178'  
    local_conn = None
    cursor = None
    try:
        local_conn = psycopg2.connect(
            host=windows_ip,
            database='velocity_railway',
            user='postgres',
            password='Password123',
            port='5432'
    )
        print("Connection successful!")
        cursor = local_conn.cursor()
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")

    # Connect to Azure and Local PostgreSQL
    #azure_conn = psycopg2.connect(**azure_conn_details)
    #local_conn = psycopg2.connect(local_conn_details)


    if cursor:
        try:
            # Proceed with the SQL query if the cursor is valid
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
            best_departure_estimate_mins VARCHAR(100),
            aimed_arrival_time VARCHAR (100)

        );
        """)
            print("Table created successfully.")
        except Exception as e:
            print(f"Error executing query: {e}")
    else:
        print("Connection or cursor initialization failed. Cannot execute the query.")


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

