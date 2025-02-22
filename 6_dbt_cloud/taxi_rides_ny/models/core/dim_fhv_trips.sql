with fhvdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
), dim_zones as (
    select * from {{ ref('dim_zone_lookup') }}
    where borough != 'Unknown'
)
select
    fhv.dispatching_base_num,
    extract(year from fhv.pickup_datetime) as year,
    extract(month from fhv.pickup_datetime) as month,
    fhv.pickup_datetime,
    fhv.dropoff_datetime,
    fhv.pickup_locationid,
    puz.borough as pickup_borough, 
    puz.zone as pickup_zone,
    fhv.dropoff_locationid,
    doz.borough as dropoff_borough, 
    doz.zone as dropoff_zone,
    fhv.sr_flag,
    fhv.affiliated_base_number
from fhvdata as fhv
inner join dim_zones as puz
on fhv.pickup_locationid = puz.locationid
inner join dim_zones as doz
on fhv.dropoff_locationid = doz.locationid
