select
    subscription_plan,
    total_sessions,
    unique_users,
    total_events,
    avg_session_minutes,
    avg_latency_ms,
    total_revenue,
    total_tokens,
    success_rate_percent,
    avg_events_per_session,
    avg_tokens_per_session,
    avg_revenue_per_session
from {{ source('ai_saas_analytics', 'session_metrics') }}