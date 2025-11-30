create table stage.transitions as
select le.event_id, le.page_url, le.page_url_path, be.event_timestamp
from raw.location_events le
    join raw.browser_events be on le.event_id = be.event_id