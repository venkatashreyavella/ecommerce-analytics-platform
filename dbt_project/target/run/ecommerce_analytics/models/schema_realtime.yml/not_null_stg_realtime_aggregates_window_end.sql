select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select window_end
from "warehouse"."staging"."stg_realtime_aggregates"
where window_end is null



      
    ) dbt_internal_test