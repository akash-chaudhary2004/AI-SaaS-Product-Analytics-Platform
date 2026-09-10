select
    event_date,
    total_revenue,
    avg_revenue_per_response,
    total_tokens,
    total_requests,
    avg_cost_per_token,
    avg_tokens_per_request
from {{ source('ai_saas_analytics', 'revenue_metrics') }}