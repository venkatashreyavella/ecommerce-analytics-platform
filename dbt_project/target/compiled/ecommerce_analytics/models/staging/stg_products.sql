select
    product_id,
    product_name,
    category,
    price,
    updated_at
from "warehouse"."raw"."raw_products"
where price >= 0