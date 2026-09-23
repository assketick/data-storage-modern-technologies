select event_key, event_id, magnitude, depth_km
from {{ ref('fact_earthquake') }}
where magnitude is null
   or magnitude not between -2 and 10
   or depth_km is null
   or depth_km not between -10 and 800
