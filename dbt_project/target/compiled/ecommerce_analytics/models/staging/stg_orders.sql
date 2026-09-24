select
    order_id,
    user_id,
    product_id,
    session_id,
    quantity,
    unit_price,
    quantity * unit_price as gross_revenue,
    order_timestamp
from "warehouse"."raw"."raw_orders"
where quantity > 0