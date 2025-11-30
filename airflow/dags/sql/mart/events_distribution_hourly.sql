create table mart.events_distribution_hourly as
with t as (
    select event_id, 
        case 
            when page_url_path like '/product_%' then 'add product'
            when page_url_path = '/home' then 'go home'
            when page_url_path = '/payment' then 'payment cart'
            when page_url_path = '/cart' then 'go cart'
            when page_url_path = '/confirmation' then 'confirmation payment'
        end as event, event_timestamp
    from stage.transitions
)
select event, date_trunc('hour', event_timestamp) date_part, count(*) cnt
from t
group by 1, 2