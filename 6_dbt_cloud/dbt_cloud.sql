-- Create Green Tripdata External Table
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-450216.raw_nyc_tripdata.ext_green_taxi`
OPTIONS (
  format='PARQUET',
  uris=['gs://nyc-tl-data-01/green/*.parquet']
);


-- Create Yellow Tripdata External Table
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-450216.raw_nyc_tripdata.ext_yellow_taxi`
OPTIONS (
  format='PARQUET',
  uris=['gs://nyc-tl-data-01/yellow/*.parquet']
);

-- Create FHV External Table
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-450216.raw_nyc_tripdata.ext_fhv`
OPTIONS (
  format='PARQUET',
  uris=['gs://nyc-tl-data-01/fhv/*.parquet']
);

-- Check Row Numbers
SELECT COUNT(*) FROM taxi-rides-ny-450216.raw_nyc_tripdata.ext_fhv;
SELECT COUNT(*) FROM taxi-rides-ny-450216.raw_nyc_tripdata.ext_green_taxi;
SELECT COUNT(*) FROM taxi-rides-ny-450216.raw_nyc_tripdata.ext_yellow_taxi;

-- View
SELECT COUNT(*) FROM taxi-rides-ny-450216.dbt_slu.dim_taxi_trips;
SELECT * FROM taxi-rides-ny-450216.dbt_slu.dim_zone_lookup LIMIT 100;

/* Adhoc Queries */
-- Question 5: Taxi Quarterly Revenue Growth
SELECT * FROM taxi-rides-ny-450216.dbt_slu.fct_taxi_quarterly_zone_revenue
WHERE year_quarter LIKE '2020%';

-- Question 6: P97/P95/P90 Taxi Monthly Fare
SELECT * FROM taxi-rides-ny-450216.dbt_slu.fct_taxi_trips_monthly_fare_p95
WHERE year = 2020 and month = 4;

SELECT service_type, COUNT(*) FROM taxi-rides-ny-450216.dbt_slu.dim_taxi_trips
WHERE extract(year from pickup_datetime) = 2020
AND extract (month from pickup_datetime) = 4
AND fare_amount > 0 AND trip_distance > 0 AND payment_type_description in ('Cash', 'Credit Card')
GROUP BY 1;


-- Question 7: Top #Nth longest P90 travel time Location for FHV
SELECT *
FROM (
  SELECT
    pickup_zone,
    dropoff_zone,
    dense_rank() over (partition by pickup_zone order by p90 desc) as rnk
  FROM taxi-rides-ny-450216.dbt_slu.fct_fhv_monthly_zone_traveltime_p90
  WHERE year = 2019 AND month = 11 
  AND pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
) t
WHERE rnk = 2;



