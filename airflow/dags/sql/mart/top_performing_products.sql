create table mart.top_performing_products as
select substring(page_url_path, strpos(page_url_path, '_') + 1) product, count(*) cnt
from stage.transitions  
where page_url_path like '/product_%'
group by 1