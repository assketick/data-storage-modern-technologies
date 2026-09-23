select
    d::date as date_key,
    extract(isodow from d)::int as iso_weekday,
    to_char(d, 'Dy') as weekday_name,
    extract(year from d)::int as year,
    extract(month from d)::int as month
from generate_series(
    '{{ var("start_date") }}'::date,
    '{{ var("end_date") }}'::date - 1,
    interval '1 day'
) as d
