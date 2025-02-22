with fhvdata as (
    select *, 
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)
select distinct
    year, 
    month, 
    pickup_locationid, 
    pickup_borough, 
    pickup_zone, 
    dropoff_locationid, 
    dropoff_borough, 
    dropoff_zone, 
    percentile_cont(trip_duration, 0.9) over (partition by year, month, pickup_locationid, dropoff_locationid) as p90
from fhvdata