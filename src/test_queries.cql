CONSISTENCY QUORUM;

SELECT * FROM air_quality.measurements WHERE station_code = 105 AND measurement_date = '2019-02-20' LIMIT 1 ALLOW FILTERING ;

SELECT station_code, pollutant_code, avg_value FROM air_quality.daily_pollutant_values_by_station WHERE station_code = 103 AND measurement_date = '2017-12-15' ALLOW FILTERING;

SELECT * FROM air_quality.measurements WHERE measurement_date = '2018-09-20' AND average_value > 1.0 ALLOW FILTERING;

SELECT pollutant_code, AVG(average_value) as avg_daily_value FROM air_quality.measurements WHERE measurement_date = '2018-08-15';

SELECT measurement_date, pollutant_code, avg_value, max_value, min_value, count_measurements
FROM air_quality.daily_pollutant_values_by_station
WHERE station_code = 115 AND measurement_date >= '2017-09-23' AND measurement_date <= '2017-10-23'
group by pollutant_code, measurement_date
ALLOW FILTERING;