select
    event_id,
    user_id,
    session_id,
    event_type,
    product_id,
    event_timestamp,
    device_type
from {{ source('raw', 'raw_clicks') }}
where event_type in ('view', 'add_to_cart', 'checkout')
