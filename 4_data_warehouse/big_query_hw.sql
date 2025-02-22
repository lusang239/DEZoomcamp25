-- Part 1: Create External Table & Cluster and Table Partition
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-450216.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-tl-data-01/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `taxi-rides-ny-450216.nytaxi.yellow_tripdata_non_partitoned`
AS SELECT * FROM `taxi-rides-ny-450216.nytaxi.external_yellow_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `taxi-rides-ny-450216.nytaxi.yellow_tripdata_partitoned`
PARTITION BY
  DATE(tpep_dropoff_datetime) AS
SELECT * FROM `taxi-rides-ny-450216.nytaxi.external_yellow_tripdata`;

-- Creating a table partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE `taxi-rides-ny-450216.nytaxi.yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `taxi-rides-ny-450216.nytaxi.external_yellow_tripdata`
);




-- Part 2: Adhoc Query
-- Count distinct number of records in external table
-- Answer: 20,332,093 (Check details in non partitioned table)

-- Estimate processed bytes for Count distinct number of records in external table
-- Answer: 155.12MB
SELECT COUNT(DISTINCT(PULocationID)) FROM `taxi-rides-ny-450216.nytaxi.external_yellow_tripdata`;

-- Estimate processed bytes for Count distinct number of records in materialized table
-- Answer: 155.12MB
SELECT COUNT(DISTINCT(PULocationID)) FROM `taxi-rides-ny-450216.nytaxi.yellow_tripdata_non_partitoned`;

-- Double processed bytes
SELECT PULocationID FROM `taxi-rides-ny-450216.nytaxi.yellow_tripdata_non_partitoned`;
SELECT PULocationID, DOLocationID FROM `taxi-rides-ny-450216.nytaxi.yellow_tripdata_non_partitoned`;

-- How many records have a fare_amount of 0?
-- Answer: 8333
SELECT COUNT(*) FROM `taxi-rides-ny-450216.nytaxi.yellow_tripdata_partitoned`
WHERE fare_amount = 0;


-- Estimated processed bytes for non partitioned table
-- Answer: 310.24MB
SELECT COUNT(DISTINCT VendorID) FROM  `taxi-rides-ny-450216.nytaxi.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';


-- Estimated processed bytes for partitioned table
-- Answer: 26.84MB
SELECT COUNT(DISTINCT VendorID) FROM  `taxi-rides-ny-450216.nytaxi.yellow_tripdata_partitoned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Bonus: Estimated processed bytes for non partitioned table
-- Answer: 0B
SELECT COUNT(*) FROM `taxi-rides-ny-450216.nytaxi.yellow_tripdata_non_partitoned`;
