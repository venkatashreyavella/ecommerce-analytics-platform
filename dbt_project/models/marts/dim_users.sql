select
    user_id,
    email,
    first_name,
    last_name,
    country,
    signup_date
from {{ ref('stg_users') }}
