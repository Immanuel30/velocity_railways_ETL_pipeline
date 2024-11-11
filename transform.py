import pandas as pd
import json
import time
from datetime import datetime
from dotenv import load_dotenv 
import os
import great_expectations as ge
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import get_context
import tempfile

#reading the data

def transformation(**kwargs):
    print("Starting transformation with response data")
    ti = kwargs['ti']
    response =ti.xcom_pull(key="raw_data", task_ids="extraction_layer")
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

    #Passing the filtered data into a dataframe
    train_columns_df = pd.DataFrame(train_columns)
    train_columns_df.reset_index(inplace=True)


    train__departure_columns = []

    for columns in response['departures']['all']:
        try:
            row ={
                'mode':columns['mode'],
                'train_uid': columns['train_uid'],
                'origin_name': columns['origin_name'],
                'operator_name':columns['operator_name'],
                'platform': columns['platform'],
                'destination_name': columns['destination_name'],
                'aimed_departure_time': columns['aimed_departure_time'],
                'expected_departure_time': columns['expected_departure_time'],
                'best_departure_estimate_mins': columns['best_departure_estimate_mins'],
                'aimed_arrival_time': columns['aimed_arrival_time']
            }
            train__departure_columns.append( row)
        except (KeyError, TypeError) as e:
            print(f"Error processing columns: {e}")     


    #passing columns into a dataframe
    train__departure_columns_df = pd.DataFrame(train__departure_columns)
    train__departure_columns_df.reset_index(inplace=True)


    #Merging both dataframes
    raw_train_schedule_df = pd.merge(train_columns_df, train__departure_columns_df, on='index', how='outer' )

    train_schedule_df = raw_train_schedule_df


    #Renaming request time column
    train_schedule_df.rename(columns= {'request_time':'request_date_time'}, inplace = True)

    #Filling missing data
    train_schedule_df['request_date_time'] = train_schedule_df['request_date_time'].ffill()
    train_schedule_df['station_name'] = train_schedule_df['station_name'].ffill()
    train_schedule_df.fillna('unknown', inplace=True)

    #Dropping the index column
    train_schedule_df.drop( axis=0, columns='index', level=None, inplace=True, errors='raise')

    #Saving file to CSV
    #try:
        #train_schedule_df.to_csv('train_schedule.csv')
        #print("Transformed data saved to CSV")
    #except Exception as e:
        #print(f"Transformed data not saved: {e}")
        

    #validating transformed data

    try:
        context = get_context()


        suite = ExpectationSuite("train_schedule_suite")


        # Set up an execution engine
        execution_engine = PandasExecutionEngine()

        # Use a Batch to wrap the DataFrame
        batch = Batch(data=train_schedule_df)

        # Step 3: Create an Expectation Suite
        #suite_name = "train_schedule_suite"
        #suite = ExpectationSuite(expectation_suite_name=suite_name)

        # Create a Validator with the Batch and ExpectationSuite
        validator = Validator(
            execution_engine=execution_engine,
            batches=[batch],
            expectation_suite=suite
        )

        # Add expectations directly to the Validator
        validator.expect_column_values_to_not_be_null(column="request_date_time")
        validator.expect_column_values_to_not_be_null(column="station_name")
        validator.expect_column_values_to_not_be_null(column="mode")
        validator.expect_column_values_to_not_be_null(column="train_uid")
        validator.expect_column_values_to_not_be_null(column="origin_name")
        validator.expect_column_values_to_not_be_null(column="operator_name")
        validator.expect_column_values_to_not_be_null(column="platform")
        validator.expect_column_values_to_not_be_null(column="destination_name")
        validator.expect_column_values_to_not_be_null(column="aimed_departure_time")
        validator.expect_column_values_to_not_be_null(column="expected_departure_time")
        validator.expect_column_values_to_not_be_null(column="best_departure_estimate_mins")
        validator.expect_column_values_to_not_be_null(column="aimed_arrival_time") 

        # Step 6: Validate the DataFrame and print the results
        transformed_results = validator.validate()
        print(f"Validation results: {transformed_results}. Data transformation successfull")
        

    except Exception as e:
        print(f"Data Validation failed: {e}")

    ti.xcom_push(key="transformed_data", value=train_schedule_df)

    return train_schedule_df



if __name__ == "__main__":
    transformation()


