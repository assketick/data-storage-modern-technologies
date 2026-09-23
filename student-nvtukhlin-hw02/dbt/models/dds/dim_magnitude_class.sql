select
    class_key,
    class_name,
    mag_min,
    mag_max
from {{ ref('mag_class') }}
