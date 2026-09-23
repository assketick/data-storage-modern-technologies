select *
from (
    select
        v.*,
        row_number() over (
            partition by event_key
            order by updated_ts desc, _slice_order desc, _row_num desc
        ) as version_rank
    from {{ ref('stg_event_versions') }} v
) ranked
where version_rank = 1
