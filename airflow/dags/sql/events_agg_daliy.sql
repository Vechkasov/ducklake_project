CREATE TABLE dds.events_agg_daily AS
SELECT date_trunc('hour', event_timestamp) dt, device_type, browser_name, os_name, count(distinct event_id) as events, count(distinct be.click_id) clicks
FROM raw.browser_events be
LEFT JOIN raw.device_events de on be.click_id=de.click_id
GROUP BY date_trunc('hour', event_timestamp), device_type, browser_name, os_name;