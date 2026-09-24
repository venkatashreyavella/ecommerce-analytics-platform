
  create view "warehouse"."staging"."stg_realtime_aggregates__dbt_tmp"
    
    
  as (
    select
    window_start,
    window_end,
    event_type,
    click_count,
    updated_at
from "warehouse"."analytics"."realtime_click_aggregates"
  );