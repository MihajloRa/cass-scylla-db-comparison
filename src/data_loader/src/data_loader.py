from concurrent.futures import ThreadPoolExecutor, as_completed
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.util import uuid_from_time
import pandas as pd
from pandas import DataFrame
import os
from connect_to_db import connect_to_db
import time

from measurement import write_average_time_to_csv


def populate_db_stations(session: Session):
    """
    Insert data from a pandas DataFrame to the stations table.
    """
    insert_stmt = session.prepare("""
        INSERT INTO stations (station_code, station_name, address, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)
    """)

    script_dir = os.path.dirname(__file__) 
    file_path = os.path.join(script_dir, 'data', 'Measurement_station_info.csv')

    df = pd.read_csv(file_path,  delimiter=",")

    arguments = df[['Station code', 'Station name(district)', 'Address', 'Latitude', 'Longitude']].values.tolist()

    execute_concurrent_with_args(session, insert_stmt, arguments, concurrency=50)



def populate_db_pollutants(session: Session):
    """
    Insert data from a pandas DataFrame to the pollutants table.
    """
    insert_stmt = session.prepare("""
        INSERT INTO pollutants (pollutant_code, pollutant_name, unit_of_measurement, good, normal, bad, very_bad)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    script_dir = os.path.dirname(__file__)  # Directory of the script
    file_path = os.path.join(script_dir, 'data', 'Measurement_item_info.csv')

    df = pd.read_csv(file_path, delimiter=",")

    arguments = df[['Item code', 'Item name', 'Unit of measurement', 'Good(Blue)', 'Normal(Green)', 'Bad(Yellow)', 'Very bad(Red)']].values.tolist()

    execute_concurrent_with_args(session, insert_stmt, arguments, concurrency=50)

def generate_timeuuid(measurement_datetime):
    """
    Generates a TimeUUID based on a datetime object.
    """
    return uuid_from_time(measurement_datetime)

def process_and_insert_data(chunk: DataFrame, session: Session):
    arguments = []
    for _,row in chunk.iterrows():
        station_code = row['Station code']
        measurement_date_time = row['Measurement date'].to_pydatetime()
        ts = uuid_from_time(measurement_date_time)  # Ensure conversion to datetime is correct
        item_code = row['Item code']
        average_value = row['Average value']
        instrument_status = row['Instrument status']
        
        argument = (station_code, measurement_date_time.date(), ts, item_code, average_value, instrument_status)
        arguments.append(argument)
    return process_chunk(arguments, session)

def process_chunk(arguments, session: Session):
    """
    Function to process each chunk and prepare the arguments for batch insertion.
    This function returns the arguments needed for execute_concurrent_with_args.
    """
    insert_stmt = session.prepare("""
        INSERT INTO measurements (station_code, measurement_date, ts, pollutant_code, average_value, instrument_status)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    execute_concurrent_with_args(session, insert_stmt, arguments, concurrency=100)
    return len(arguments)

def populate_db_measurements(session: Session):
    total_time = 0
    chunksize = 20000

    script_dir = os.path.dirname(__file__) 
    file_path = os.path.join(script_dir, 'data', 'Measurement_info.csv')
    
    with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers based on your system's capabilities
        futures = []
        processed = 0
        start_time = time.time()  # Move timing outside the loop

        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            # Submit the processing of each chunk to the executor
            chunk['Measurement date'] = pd.to_datetime(chunk['Measurement date'])
            futures.append(executor.submit(process_and_insert_data, chunk, session))

        for future in as_completed(futures):
            processed += future.result()
            print(f"Chunk processed, currently processed: {processed}")

        end_time = time.time()
        total_time += end_time - start_time
        write_average_time_to_csv('cassandra.db_insertion_times.csv', 'populate_db_measurements', total_time, processed, chunksize)




if __name__=="__main__":
    with  connect_to_db() as session:
        populate_db_stations(session)
        populate_db_pollutants(session)
        populate_db_measurements(session)