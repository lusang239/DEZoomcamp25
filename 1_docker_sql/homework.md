# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1
- 24.2.1
- 23.3.1
- 23.2.1

```
docker run -it --entrypoint=bash python:3.12.8
root@8f412043be43:/# pip3 -V
root@8f412043be43:/# pip3 --version
```
Answer = 24.3.1

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

If there are more than one answers, select only one of them

Answer= db:5432


##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189
- 104,793;  201,407;  110,612;  27,831;  35,281
- 104,793;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202


Query:
```
SELECT 
    category, 
    COUNT(1) AS nums
FROM 
    (
        SELECT 
            CASE 
                WHEN trip_distance <= 1 THEN 'Up to 1 mile'
                WHEN trip_distance BETWEEN 1 AND 3 THEN 'between 1 and 3'
                WHEN trip_distance BETWEEN 3 AND 7 THEN 'between 3 and 7'
                WHEN trip_distance BETWEEN 7 AND 10 THEN 'between 7 and 10'
                ELSE 'Over 10 miles'
            END AS category,
			lpep_pickup_datetime,
			lpep_dropoff_datetime
        FROM green_tripdata
    ) t
WHERE TO_CHAR(lpep_pickup_datetime, 'YYYY-MM') = '2019-10'
AND TO_CHAR(lpep_dropoff_datetime, 'YYYY-MM') = '2019-10'
GROUP BY category;
```
```
"category"	"nums"
"between 1 and 3"	198924
"between 3 and 7"	109603
"between 7 and 10"	27678
"Over 10 miles"	35189
"Up to 1 mile"	104802
```
Answer = 104,802;  198,924;  109,603;  27,678;  35,189

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

Query:
```
SELECT DISTINCT DATE(lpep_pickup_datetime) AS date 
FROM green_tripdata 
WHERE Trip_distance = (SELECT MAX(Trip_distance) FROM green_tripdata);
```
```
+------------+
| date       |
|------------|
| 2019-10-31 |
+------------+
```
Answer = 2019-10-31

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

Query:
```
SELECT z."Borough", z."Zone", z."service_zone"
FROM green_tripdata g
JOIN zones z ON g."PULocationID" = z."LocationID"
WHERE DATE(g."lpep_pickup_datetime") = '2019-10-18'
GROUP BY 1,2,3
HAVING SUM(g."total_amount") > 13000;
```
```
+-----------+---------------------+--------------+
| Borough   | Zone                | service_zone |
|-----------+---------------------+--------------|
| Manhattan | East Harlem North   | Boro Zone    |
| Manhattan | East Harlem South   | Boro Zone    |
| Manhattan | Morningside Heights | Boro Zone    |
+-----------+---------------------+--------------+
```
Answer= East Harlem North, East Harlem South, Morningside Heights

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

Query:
```
SELECT zdo."Zone", SUM(g."tip_amount") tip_amount
FROM green_tripdata g
JOIN zones zpu ON g."PULocationID" = zpu."LocationID"
JOIN zones zdo ON g."DOLocationID" = zdo."LocationID"
WHERE TO_CHAR(g."lpep_pickup_datetime", 'YYYY-MM') = '2019-10'
AND zpu."Zone" = 'East Harlem North'
GROUP BY 1
ORDER BY SUM(g."tip_amount") DESC
LIMIT 10;
```
```
+-----------------------+--------------------+
| Zone                  | tip_amount         |
|-----------------------+--------------------|
| Upper East Side North | 4935.360000000037  |
| East Harlem South     | 4076.4399999999982 |
| Morningside Heights   | 3369.359999999996  |
| Yorkville West        | 3322.15000000001   |
| Upper West Side North | 2942.3200000000015 |
| Upper West Side South | 2337.9500000000007 |
| Central Harlem        | 2126.3600000000006 |
| Yorkville East        | 1854.1599999999971 |
| Manhattan Valley      | 1644.8199999999981 |
| LaGuardia Airport     | 1603.1100000000001 |
+-----------------------+--------------------+
```
Answer: East Harlem South

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

Answer= terraform init, terraform apply -auto-approve, terraform destroy
