select a.source_system, a.net, a.network_key as version_a, b.network_key as version_b,
       a.valid_from as a_from, a.valid_to as a_to, b.valid_from as b_from, b.valid_to as b_to
from {{ ref('dim_network') }} a
join {{ ref('dim_network') }} b
    on a.source_system = b.source_system
    and a.net = b.net
    and a.network_key < b.network_key
    and a.valid_from < b.valid_to
    and b.valid_from < a.valid_to
