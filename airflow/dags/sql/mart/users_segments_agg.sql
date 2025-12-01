CREATE TABLE mart.users_segments_agg as 
SELECT os_name, device_type, utm_source, count(distinct user_custom_id) users
FROM stage.devices d
INNER JOIN stage.conversion c on d.session_id=c.session_id
GROUP BY os_name, device_type,utm_source