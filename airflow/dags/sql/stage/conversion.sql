create table stage.conversion as
with merge as (
    select be.click_id session_id, max(le.event_id) event_id
    from raw.location_events le
        join raw.browser_events be on be.event_id = le.event_id
    group by 1
)
select m.session_id, le.referer_url, le.referer_medium, le.utm_medium, le.utm_source, 
    le.utm_content, le.utm_campaign
from merge m
    join raw.location_events le on le.event_id = m.event_id
order by session_id