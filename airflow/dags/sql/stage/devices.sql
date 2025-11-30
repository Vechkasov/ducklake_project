create table stage.devices as
select distinct click_id session_id, os_name, os_timezone, device_type, 
    user_custom_id, user_domain_id
from raw.device_events