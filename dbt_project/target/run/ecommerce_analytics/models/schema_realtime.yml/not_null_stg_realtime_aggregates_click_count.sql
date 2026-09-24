select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select click_count
from "warehouse"."staging"."stg_realtime_aggregates"
where click_count is null



      
    ) dbt_internal_test