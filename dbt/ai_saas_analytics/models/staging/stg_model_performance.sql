select
    model_name,
    total_events,
    unique_users,
    total_sessions,
    avg_latency_ms,
    min_latency_ms,
    max_latency_ms,
    total_revenue,
    avg_response_cost,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    success_rate_percent,
    avg_tokens_per_request,
    avg_revenue_per_request
from {{ source('ai_saas_analytics', 'model_performance') }}