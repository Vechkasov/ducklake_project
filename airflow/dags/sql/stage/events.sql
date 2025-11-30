create table stage.events as
select distinct click_id session_id, event_id, event_timestamp
from raw.browser_events