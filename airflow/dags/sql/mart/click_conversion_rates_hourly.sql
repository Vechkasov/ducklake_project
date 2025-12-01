create table mart.click_conversion_rates_hourly as
select c.utm_source, date_trunc('hour', p.event_timestamp) date_part, sum(p.product_count) sm
from stage.purchases p
    join stage.conversion c on p.session_id = c.session_id
group by 1, 2