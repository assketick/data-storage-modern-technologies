select event_date, magnitude_class, sum(events) as events
from mart.daily_quakes
group by 1, 2
order by 1, 2;

select event_date, net,
       sum(events) as events,
       round(100.0 * sum(events) / sum(sum(events)) over (partition by event_date), 1) as share_pct
from mart.daily_quakes
group by 1, 2
order by 1, events desc, net;

select event_date,
       max(max_magnitude) as max_magnitude,
       round(sum(avg_depth_km * events) / sum(events), 2) as avg_depth_km,
       sum(events) as events
from mart.daily_quakes
group by 1
order by 1;
