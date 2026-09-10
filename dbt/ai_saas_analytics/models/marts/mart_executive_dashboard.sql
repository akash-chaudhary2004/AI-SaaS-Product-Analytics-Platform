select
    total_users,
    total_sessions,
    total_events,
    total_revenue,
    total_tokens,
    avg_latency_ms,
    success_rate_percent,
    avg_events_per_user,
    avg_sessions_per_user,
    avg_tokens_per_event,
    avg_revenue_per_event
from {{ ref('stg_executive_dashboard') }}