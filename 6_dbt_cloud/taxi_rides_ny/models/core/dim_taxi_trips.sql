{{ config( materialized='table' ) }}

with green_tripdata as (
    select *,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), yellow_tripdata as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
), dim_zones as (
    select * from {{ ref('dim_zone_lookup') }}
    where borough != 'Unknown'
)
select tp.tripid, 
    tp.vendorid, 
    tp.service_type,
    tp.ratecodeid, 
    tp.pickup_locationid, 
    puz.borough as pickup_borough, 
    puz.zone as pickup_zone, 
    tp.dropoff_locationid,
    doz.borough as dropoff_borough, 
    doz.zone as dropoff_zone,  
    tp.pickup_datetime, 
    tp.dropoff_datetime, 
    tp.store_and_fwd_flag, 
    tp.passenger_count, 
    tp.trip_distance, 
    tp.trip_type, 
    tp.fare_amount, 
    tp.extra, 
    tp.mta_tax, 
    tp.tip_amount, 
    tp.tolls_amount, 
    tp.ehail_fee, 
    tp.improvement_surcharge, 
    tp.total_amount, 
    tp.payment_type, 
    tp.payment_type_description    
from trips_unioned tp
inner join dim_zones as puz
on tp.pickup_locationid = puz.locationid
inner join dim_zones as doz
on tp.dropoff_locationid = doz.locationid