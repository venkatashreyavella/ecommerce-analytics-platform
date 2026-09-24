select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_type
from "warehouse"."staging"."stg_realtime_aggregates"
where event_type is null



      
    ) dbt_internal_test