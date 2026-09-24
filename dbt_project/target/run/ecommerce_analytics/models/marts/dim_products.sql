
  
    

  create  table "warehouse"."analytics"."dim_products__dbt_tmp"
  
  
    as
  
  (
    select
    product_id,
    product_name,
    category,
    price,
    updated_at
from "warehouse"."staging"."stg_products"
  );
  