from connect_to_db import connect_to_db
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from cassandra.cluster import Session
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import argparse
import csv
import time
from datetime import datetime

global thresholds
global stations


def calculate_air_quality_label(avg_value, thresholds):
    if avg_value <= thresholds['good']:
        return 'good'
    elif avg_value <= thresholds['normal']:
        return 'normal'
    elif avg_value <= thresholds['bad']:
        return 'bad'
    else:
        return 'very_bad'
    
def fetch_all_thresholds(session):
    query = "SELECT pollutant_code, good, normal, bad, very_bad FROM pollutants"
    results = session.execute(query)
    return results.all()

def get_thresholds_for_pollutant(pollutant_code):
    global thresholds
    return thresholds.loc[pollutant_code]

def execute_batch_inserts(session: Session, batch_data: pd.DataFrame, station_code):
    max_batch_size=10000
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    insert_query = session.prepare("""
        INSERT INTO daily_pollutant_values_by_station (station_code, measurement_date, pollutant_code, avg_value, max_value, min_value, air_quality_label, count_measurements)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    batch_count = 0

    for _,row in batch_data.iterrows():
        thresholds_row = thresholds.loc[row['pollutant_code']]

        air_quality_label = calculate_air_quality_label(row['mean'], thresholds_row)

        batch.add(insert_query, (station_code, row['measurement_date'], row['pollutant_code'], row['mean'], row['max'], row['min'], air_quality_label, row['count']))
        batch_count += 1

        if batch_count >= max_batch_size:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_count = 0

    session.execute(batch)

def execute_concurrent_inserts(session: Session, batch_data, station_code):
    insert_query = session.prepare("""
        INSERT INTO daily_pollutant_values_by_station (station_code, measurement_date, pollutant_code, avg_value, max_value, min_value, air_quality_label, count_measurements)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    start_time = time.time()  # Start timing


    # Prepare arguments for each insert operation
    arguments = []
    for _,row in batch_data.iterrows():
        # Fetch thresholds directly from the global DataFrame
        thresholds_row = thresholds.loc[row['pollutant_code']]

        # Calculate air quality label
        air_quality_label = calculate_air_quality_label(row['mean'], thresholds_row)

        # Append arguments for this row
        arguments.append((station_code, row['measurement_date'], row['pollutant_code'], row['mean'], row['max'], row['min'], air_quality_label, row['count']))

    # Execute insert operations concurrently
    execute_concurrent_with_args(session, insert_query, arguments, concurrency=100)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(elapsed_time)


def populate_daily_pollutant_aggregations_for_station(session: Session, station_code, start_date, end_date):
    measurements = session.execute("""
        SELECT average_value, pollutant_code, measurement_date
        FROM measurements
        WHERE station_code = %s AND measurement_date >= %s AND measurement_date <= %s ALLOW FILTERING
    """, [station_code, start_date, end_date])


    df_measurements = pd.DataFrame(measurements, columns=['average_value', 'pollutant_code', 'measurement_date'])
    aggregation = df_measurements.groupby(['pollutant_code', 'measurement_date'])['average_value'].agg(['mean', 'min', 'max', 'count']).reset_index()

    execute_concurrent_inserts(session, aggregation, station_code)



def main():
    parser = argparse.ArgumentParser(description='Populate daily aggregations for air quality measurements.')
    parser.add_argument('--station_code', type=int, help='Station code to aggregate data for. Ignored if -a is provided.')
    parser.add_argument('--start_date', type=str, help='Start date for the aggregation range, format YYYY-MM-DD.')
    parser.add_argument('--end_date', type=str, help='End date for the aggregation range, format YYYY-MM-DD.')
    parser.add_argument('-a', '--all_stations', action='store_true', help='Aggregate data for all stations.')

    args = parser.parse_args()

    if not args.start_date or not args.end_date:
        parser.error('The --start_date and --end_date arguments are required.')

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    with connect_to_db() as session:
        global stations 
        if args.all_stations:
            fetched_stations = session.execute("SELECT station_code FROM stations").all()
            stations = [station.station_code for station in fetched_stations]        
        elif args.station_code:
            stations = [args.station_code]
        else:
            parser.error('No station code provided and -a flag not set. Specify a station code or use -a for all stations.')

        global thresholds
        thresholds = fetch_all_thresholds(session)
        thresholds_df = pd.DataFrame(thresholds, columns=['pollutant_code', 'good', 'normal', 'bad', 'very_bad'])
        thresholds_df.set_index('pollutant_code', inplace=True)
        thresholds = thresholds_df

        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

        for station in stations:
            populate_daily_pollutant_aggregations_for_station(session, station, start_date, end_date)

        

if __name__=="__main__":
    main()