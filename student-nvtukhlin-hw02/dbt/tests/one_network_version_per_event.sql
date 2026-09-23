select e.event_key, e.event_id, e.source_system, e.net, count(*) as matching_versions
from {{ ref('stg_events_latest') }} e
join {{ ref('dim_network') }} n
    on n.source_system = e.source_system
    and n.net = e.net
    and e.event_ts >= n.valid_from
    and e.event_ts < n.valid_to
where e.event_type = 'earthquake'
group by 1, 2, 3, 4
having count(*) > 1
