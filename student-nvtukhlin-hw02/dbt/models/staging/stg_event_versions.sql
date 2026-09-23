select
    md5(_source_system || '|' || id) as event_key,
    _source_system as source_system,
    id as event_id,
    nullif(time, '')::timestamptz as event_ts,
    (nullif(time, '')::timestamptz at time zone 'UTC')::date as event_date,
    nullif(updated, '')::timestamptz as updated_ts,
    nullif(latitude, '')::numeric as latitude,
    nullif(longitude, '')::numeric as longitude,
    nullif(depth, '')::numeric as depth_km,
    nullif(mag, '')::numeric as magnitude,
    nullif(magtype, '') as mag_type,
    nullif(net, '') as net,
    nullif(type, '') as event_type,
    nullif(status, '') as status,
    nullif(place, '') as place,
    _slice_id,
    _slice_order,
    _row_num,
    _is_synthetic
from {{ source('raw', 'usgs_events') }}
where (nullif(time, '')::timestamptz at time zone 'UTC')::date >= '{{ var("start_date") }}'::date
  and (nullif(time, '')::timestamptz at time zone 'UTC')::date < '{{ var("end_date") }}'::date
