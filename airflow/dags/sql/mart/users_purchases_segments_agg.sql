CREATE TABLE mart.users_purchases_segments_agg AS
select device_type, utm_source, os_name, sum(products) products
FROM
(select date_trunc('hour', event_timestamp) hr, user_custom_id,device_type, utm_source, os_name, sum(product_count) products
from stage.purchases p
inner join stage.devices d on p.session_id=d.session_id
inner join stage.conversion c on p.session_id=c.session_id
group by date_trunc('hour', event_timestamp), user_custom_id,device_type, os_name, utm_source)
GROUP BY device_type, utm_source, os_name