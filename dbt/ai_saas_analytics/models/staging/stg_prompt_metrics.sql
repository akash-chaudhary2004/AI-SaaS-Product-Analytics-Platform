select
    prompt_category,
    total_prompts,
    unique_users,
    total_sessions,
    avg_latency_ms,
    min_latency_ms,
    max_latency_ms,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    total_revenue,
    avg_response_cost,
    success_rate_percent,
    avg_tokens_per_prompt,
    avg_revenue_per_prompt
from {{ source('ai_saas_analytics', 'prompt_metrics') }}