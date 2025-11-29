CREATE TABLE dds.medium_purchases_agg_daily AS
with events as (
SELECT event_id, click_id, event_timestamp
FROM raw.browser_events),
urls as (
SELECT event_id, utm_medium
from raw.location_events)
SELECT date_trunc('day', event_timestamp) as dt, utm_medium, count(distinct click_id) as purchases
FROM events e 
INNER JOIN urls u on e.event_id=u.event_id
GROUP BY date_trunc('day', event_timestamp), utm_medium