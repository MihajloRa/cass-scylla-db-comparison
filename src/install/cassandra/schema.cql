CREATE KEYSPACE IF NOT EXISTS air_quality WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

USE air_quality;

CREATE TABLE IF NOT EXISTS air_quality.measurements (
    station_code int,
    measurement_date date,
    ts timeuuid,
    pollutant_code int,
    average_value float,
    instrument_status int,
    PRIMARY KEY ((station_code, measurement_date, pollutant_code), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
AND COMPACTION = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '1',
    'compaction_window_unit': 'DAYS'
};

CREATE TABLE IF NOT EXISTS air_quality.pollutants (
    pollutant_code int PRIMARY KEY,
    pollutant_name text,
    unit_of_measurement text,
    good float,
    normal float,
    bad float,
    very_bad float
);

CREATE TABLE IF NOT EXISTS air_quality.stations (
    station_code int PRIMARY KEY,
    station_name text,
    address text,
    latitude float,
    longitude float
);

CREATE TABLE IF NOT EXISTS air_quality.daily_pollutant_values_by_station (
    station_code int,
    measurement_date date,
    pollutant_code int,
    avg_value float,
    max_value float,
    min_value float,
    air_quality_label text,
    count_measurements int,
    PRIMARY KEY ((pollutant_code, measurement_date), station_code)
) WITH CLUSTERING ORDER BY (station_code ASC);


CREATE INDEX on measurements (measurement_date);
CREATE INDEX on daily_pollutant_values_by_station (measurement_date);