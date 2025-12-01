create table stage.purchases as
with events AS (
    select 
        be.click_id,
        be.event_id,
        be.event_timestamp,
        le.page_url_path,
        case when le.page_url_path LIKE '/product_%' then 1 else 0 end is_product,
        case when le.page_url_path = '/confirmation' then 1 else 0 end is_confirmation
    from raw.browser_events be
    join raw.location_events le ON be.event_id = le.event_id
),
confirmation_events AS (
    select click_id, event_id, event_timestamp
    from events
    WHERE is_confirmation = 1
),
product_counts AS (
    select ce.event_id, ce.event_timestamp, count(*) product_count
    from confirmation_events ce
    left join events pe on pe.click_id = ce.click_id
        and pe.is_product = 1 
        and pe.event_timestamp < ce.event_timestamp
    GROUP BY ce.event_id, ce.event_timestamp
), cumulative as (
    select 
        be.click_id,
        be.event_id,
        be.event_timestamp,
        COALESCE(pc.product_count, 0) product_count
    from raw.browser_events be
    join raw.location_events le on be.event_id = le.event_id
    left join product_counts pc on be.event_id = pc.event_id
    where le.page_url_path = '/confirmation'
), result as (
    select c1.click_id session_id, c1.event_timestamp,
        c1.product_count - coalesce((
            select max(c2.product_count) 
            from cumulative c2 
            where c2.event_timestamp < c1.event_timestamp 
                and c1.click_id = c2.click_id), 0) product_count
    from cumulative c1
    order by c1.click_id, c1.event_timestamp
)

select *
from result