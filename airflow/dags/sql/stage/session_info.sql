create table stage.session_info as
select distinct click_id session_id, browser_name, browser_user_agent, browser_language
from raw.browser_events