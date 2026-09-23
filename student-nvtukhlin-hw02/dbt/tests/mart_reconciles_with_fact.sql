with f as (
    select event_date, count(*) as events from {{ ref('fact_earthquake') }} group by 1
),
m as (
    select event_date, sum(events) as events from {{ ref('mart_daily_quakes') }} group by 1
)
select coalesce(f.event_date, m.event_date) as event_date, f.events as fact_events, m.events as mart_events
from f
full join m on m.event_date = f.event_date
where f.events is distinct from m.events
