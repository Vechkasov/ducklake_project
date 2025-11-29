CREATE TABLE dds.users_agg_daily AS
with events as (
    SELECT event_id, click_id, event_timestamp
    FROM raw.browser_events),
    urls as (
    SELECT event_id, utm_medium
    from raw.location_events),
    users as (SELECT click_id, user_custom_id, device_type, os_name
    from raw.device_events),
    geo as (SELECT click_id, geo_country
    from raw.geo_events)
SELECT date_trunc('day', event_timestamp) as dt, utm_medium, geo_country, device_type, os_name, count(distinct user_custom_id) as users
FROM events e 
INNER JOIN urls u on e.event_id=u.event_id
INNER JOIN users us on e.click_id=us.click_id
INNER JOIN geo g on e.click_id=g.click_id
GROUP BY date_trunc('day', event_timestamp),geo_country, utm_medium, device_type, os_name