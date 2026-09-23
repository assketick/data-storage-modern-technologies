select
    f.event_date,
    c.class_key,
    c.class_name as magnitude_class,
    f.source_system,
    n.net,
    n.network_name,
    count(*) as events,
    round(avg(f.magnitude), 3) as avg_magnitude,
    max(f.magnitude) as max_magnitude,
    round(avg(f.depth_km), 2) as avg_depth_km
from {{ ref('fact_earthquake') }} f
left join {{ ref('dim_network') }} n on n.network_key = f.network_key
left join {{ ref('dim_magnitude_class') }} c on c.class_key = f.mag_class_key
group by 1, 2, 3, 4, 5, 6
