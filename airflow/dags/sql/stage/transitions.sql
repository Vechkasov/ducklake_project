create table stage.transitions as
select le.event_id, be.click_id as session_id, le.page_url, be.browser_name, le.page_url_path, be.event_timestamp
from raw.location_events le
    join raw.browser_events be on le.event_id = be.event_id