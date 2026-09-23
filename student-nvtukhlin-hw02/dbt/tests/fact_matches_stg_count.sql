with s as (
    select count(*) as c from {{ ref('stg_events_latest') }} where event_type = 'earthquake'
),
f as (
    select count(*) as c from {{ ref('fact_earthquake') }}
)
select s.c as stg_rows, f.c as fact_rows
from s, f
where s.c <> f.c
