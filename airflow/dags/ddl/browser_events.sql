CREATE TABLE IF NOT EXISTS  raw.browser_events
        (click_id uuid ,
        event_id uuid,
        event_timestamp timestamp,
        event_type varchar,
        browser_name varchar,
        browser_language varchar,
        browser_user_agent varchar
        );

CREATE TABLE IF NOT EXISTS  raw.device_events
        (click_id uuid ,
        device_type varchar,
        device_is_mobile bool,
        user_custom_id varchar,
        user_domain_id varchar,
        os varchar,
        os_name varchar,
        os_timezone varchar);


CREATE TABLE IF NOT EXISTS raw.geo_events
        (
  click_id  UUID,
geo_country VARCHAR,
geo_timezone VARCHAR ,
geo_region_name VARCHAR ,
geo_latitude    VARCHAR ,
geo_longitude   VARCHAR  
        );

CREATE TABLE IF NOT EXISTS raw.location_events
(event_id  UUID ,
page_url VARCHAR ,
page_url_path  VARCHAR  ,
referer_url    VARCHAR   ,
referer_medium VARCHAR  ,
utm_medium     VARCHAR  ,
utm_source     VARCHAR  ,
utm_content    VARCHAR  ,
utm_campaign   VARCHAR );
