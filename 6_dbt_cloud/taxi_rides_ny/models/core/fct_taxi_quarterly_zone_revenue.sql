with trip_data as (
  select
    service_type,
    extract(YEAR from pickup_datetime) as year,
    extract(QUARTER from pickup_datetime) as quarter,
    sum(total_amount) as revenue_quarterly_total_amount
  from {{ ref('dim_taxi_trips') }}
  group by 1,2,3
)
select
  curr.service_type,
  concat(curr.year, "-Q", curr.quarter) as year_quarter,
  curr.revenue_quarterly_total_amount,
  prev.revenue_quarterly_total_amount as previous_quarter_total_amount,
  round(safe_divide(curr.revenue_quarterly_total_amount - prev.revenue_quarterly_total_amount, prev.revenue_quarterly_total_amount)*100,2) as yoy_growth_pct
from trip_data curr
inner join trip_data prev
on curr.service_type = prev.service_type and curr.year = prev.year+1 and curr.quarter = prev.quarter
order by 1,2