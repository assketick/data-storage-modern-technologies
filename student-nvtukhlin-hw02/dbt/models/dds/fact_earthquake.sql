select
    e.event_key,
    e.source_system,
    e.event_id,
    e.event_ts,
    e.event_date,
    n.network_key,
    c.class_key as mag_class_key,
    e.magnitude,
    e.depth_km,
    e.latitude,
    e.longitude,
    e.mag_type,
    e.status,
    e.updated_ts,
    e.place
from {{ ref('stg_events_latest') }} e
left join {{ ref('dim_network') }} n
    on n.source_system = e.source_system
    and n.net = e.net
    and e.event_ts >= n.valid_from
    and e.event_ts < n.valid_to
left join {{ ref('dim_magnitude_class') }} c
    on e.magnitude >= c.mag_min
    and e.magnitude < c.mag_max
where e.event_type = 'earthquake'
