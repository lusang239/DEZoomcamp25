with trip_data as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('dim_taxi_trips') }}
    where fare_amount>0 and trip_distance>0 and payment_type_description in ('Cash', 'Credit Card')
)
select distinct service_type, year, month,
    percentile_cont(fare_amount, 0.90) over (partition by service_type, year, month) p90,
    percentile_cont(fare_amount, 0.95) over (partition by service_type, year, month) p95,
    percentile_cont(fare_amount, 0.97) over (partition by service_type, year, month) p97
from trip_data