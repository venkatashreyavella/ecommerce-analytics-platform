select
    window_start,
    window_end,
    event_type,
    click_count,
    updated_at
from {{ source('analytics', 'realtime_click_aggregates') }}
