select event_date, class_key, source_system, net, network_name, count(*) as duplicates
from {{ ref('mart_daily_quakes') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
