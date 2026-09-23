create schema if not exists mart;

create table if not exists mart.daily_quakes (like mart_candidate.mart_daily_quakes);

create table if not exists mart.publication_log (
    run_id text,
    start_date date,
    end_date date,
    rows_published int,
    checksum text,
    published_at timestamptz default now()
);

delete from mart.daily_quakes
where event_date >= %(start_date)s::date and event_date < %(end_date)s::date;

insert into mart.daily_quakes
select * from mart_candidate.mart_daily_quakes
where event_date >= %(start_date)s::date and event_date < %(end_date)s::date;

insert into mart.publication_log (run_id, start_date, end_date, rows_published, checksum)
select
    %(run_id)s,
    %(start_date)s::date,
    %(end_date)s::date,
    count(*),
    coalesce(md5(string_agg(t::text, '|' order by t::text)), md5(''))
from mart.daily_quakes t
where event_date >= %(start_date)s::date and event_date < %(end_date)s::date;
