create table mart.events_distribution_hourly as
with t as (
    select event_id, e.session_id,os_name, device_type, browser_name,
        case 
            when page_url_path like '/product_%' then 'add product'
            when page_url_path = '/home' then 'go home'
            when page_url_path = '/payment' then 'payment cart'
            when page_url_path = '/cart' then 'go cart'
            when page_url_path = '/confirmation' then 'confirmation payment'
        end as event, event_timestamp
    from stage.transitions e
    INNER JOIN stage.devices d on e.session_id=d.session_id
    
)
select event, date_trunc('hour', event_timestamp) date_part, os_name, device_type, browser_name, count(*) cnt
from t
GROUP BY date_trunc('hour', event_timestamp), os_name, device_type, browser_name, event