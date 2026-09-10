select
    event_date,
    daily_active_users,
    total_sessions,
    total_events,
    avg_latency_ms,
    total_revenue,
    total_tokens,
    success_rate_percent,
    avg_events_per_user,
    avg_sessions_per_user,
    avg_tokens_per_event
from {{ ref('stg_user_activity') }}