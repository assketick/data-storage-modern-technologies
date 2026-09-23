select
    _source_system as source_system,
    net,
    network_name,
    (valid_from::timestamp at time zone 'UTC') as valid_from,
    (valid_to::timestamp at time zone 'UTC') as valid_to,
    _is_synthetic
from {{ source('raw', 'network_history') }}
