select
    md5(source_system || '|' || net || '|' || valid_from::text) as network_key,
    source_system,
    net,
    network_name,
    valid_from,
    valid_to,
    valid_to >= '9999-12-31'::timestamptz as is_current
from {{ ref('stg_network_history') }}
