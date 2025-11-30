create table mart.campaign_purchase_analysis_hourly as
select c.utm_campaign, c.utm_medium, date_trunc('hour', p.event_timestamp) date_part, sum(p.product_count) sm
from stage.purchases p
    join stage.conversion c on p.session_id = c.session_id
group by 1, 2, 3