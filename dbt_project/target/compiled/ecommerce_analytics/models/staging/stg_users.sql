select
    user_id,
    lower(email) as email,
    first_name,
    last_name,
    country,
    signup_date
from "warehouse"."raw"."raw_users"