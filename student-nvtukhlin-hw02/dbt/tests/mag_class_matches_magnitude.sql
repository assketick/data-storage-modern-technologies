select f.event_key, f.event_id, f.magnitude, c.class_name, c.mag_min, c.mag_max
from {{ ref('fact_earthquake') }} f
join {{ ref('dim_magnitude_class') }} c on c.class_key = f.mag_class_key
where f.magnitude < c.mag_min or f.magnitude >= c.mag_max
