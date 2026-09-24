
  
    

  create  table "warehouse"."analytics"."dim_users__dbt_tmp"
  
  
    as
  
  (
    select
    user_id,
    email,
    first_name,
    last_name,
    country,
    signup_date
from "warehouse"."staging"."stg_users"
  );
  