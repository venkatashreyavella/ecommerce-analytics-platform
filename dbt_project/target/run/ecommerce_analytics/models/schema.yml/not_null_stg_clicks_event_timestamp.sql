select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_timestamp
from "warehouse"."staging"."stg_clicks"
where event_timestamp is null



      
    ) dbt_internal_test