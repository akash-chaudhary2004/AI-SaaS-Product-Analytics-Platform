select
    event_date,
    total_revenue,
    avg_revenue_per_response,
    total_tokens,
    total_requests,
    avg_cost_per_token,
    avg_tokens_per_request
from {{ ref('stg_revenue_metrics') }}