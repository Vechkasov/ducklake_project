create table mart.purchased_products_count_hourly as
select date_trunc('hour', event_timestamp) date_part, sum(product_count) cnt
from stage.purchases
group by 1