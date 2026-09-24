
  create view "warehouse"."staging"."stg_users__dbt_tmp"
    
    
  as (
    select
    user_id,
    lower(email) as email,
    first_name,
    last_name,
    country,
    signup_date
from "warehouse"."raw"."raw_users"
  );