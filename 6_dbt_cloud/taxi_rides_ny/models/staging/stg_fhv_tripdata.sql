with fhvdata as (
    select * from {{ source('raw', 'ext_fhv') }}
    where dispatching_base_num is not null 
)
select
    dispatching_base_num,
    timestamp_millis(cast(pickup_datetime/1000000 as integer)) as pickup_datetime,
    timestamp_millis(cast(dropOff_datetime/1000000 as integer)) as dropoff_datetime,
    {{ dbt.safe_cast('PULocationID', api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast('DOLocationID', api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast('SR_Flag', api.Column.translate_type("integer")) }} as sr_flag,
    affiliated_base_number
from fhvdata