select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select window_start
from "warehouse"."staging"."stg_realtime_aggregates"
where window_start is null



      
    ) dbt_internal_test