select
    feedback,
    total_feedback,
    unique_users,
    total_sessions,
    avg_rating,
    avg_latency_ms,
    total_revenue,
    total_tokens,
    success_rate_percent,
    avg_tokens_per_feedback,
    avg_revenue_per_feedback
from {{ source('ai_saas_analytics', 'feedback_metrics') }}