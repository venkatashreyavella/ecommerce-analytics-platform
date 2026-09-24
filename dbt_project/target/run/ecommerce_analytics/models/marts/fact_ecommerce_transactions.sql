
  
    

  create  table "warehouse"."analytics"."fact_ecommerce_transactions__dbt_tmp"
  
  
    as
  
  (
    select
    orders.order_id,
    orders.order_timestamp,
    orders.user_id,
    orders.product_id,
    orders.session_id,
    orders.quantity,
    orders.unit_price,
    orders.gross_revenue,
    coalesce(sum(aggregates.click_count), 0) as session_window_clicks,
    max(aggregates.window_end) as latest_click_window_end
from "warehouse"."staging"."stg_orders" as orders
left join "warehouse"."staging"."stg_realtime_aggregates" as aggregates
    on aggregates.window_start <= orders.order_timestamp
    and aggregates.window_end >= orders.order_timestamp
    and aggregates.event_type = 'checkout'
group by
    orders.order_id,
    orders.order_timestamp,
    orders.user_id,
    orders.product_id,
    orders.session_id,
    orders.quantity,
    orders.unit_price,
    orders.gross_revenue
  );
  