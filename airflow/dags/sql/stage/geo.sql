create table stage.geo as
select distinct click_id session_id, geo_latitude, geo_longitude, geo_country, geo_timezone, 
    geo_region_name, ip_address
from raw.geo_events