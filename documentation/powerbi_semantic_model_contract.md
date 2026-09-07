# Power BI Semantic Model Contract

## Status

POWER BI BASELINE: FROZEN
CLOUD MIGRATION: NOT STARTED
POWER BI SOURCE MIGRATION: NOT STARTED

Baseline date: 2026-09-07

---

# 1. Power BI Report

Report: AI SaaS Product Analytics

Pages:

1. Executive Overview
2. User Analytics
3. AI Model Performance
4. Revenue & Subscription
5. Product Usage
6. Cost & Efficiency

---

# 2. Power BI Fact Tables

- FactExecutive
- FactUserActivity
- FactAICost
- FactSubscription
- FactCountry
- FactModel
- FactPrompt
- FactFeedback
- FactSession

---

# 3. Power BI Dimension Tables

- DimDate
- DimPlan
- DimCountry
- DimModel
- DimPrompt

---

# 4. Measures Table

- _Measures

---

# 5. Known Important Metrics

## Revenue

- Total Revenue
- Payment Revenue
- Revenue by Subscription Plan
- Net Revenue after AI Cost
- Gross Contribution

## Users

- Total Users
- Total Active Users

## Sessions

- Total Sessions

## AI Usage

- Total AI Events
- Total AI Responses
- Total AI Tokens
- AI Adoption

## AI Cost

- Total AI Cost
- AI Cost % of Revenue
- Cost Efficiency

## Performance

- Average Latency
- Success Rate

## Business Insights

- Revenue Driver Insight
- AI Adoption Insight
- Cost Efficiency Insight
- Model Concentration Insight

---

# 6. Known Source Fields

## FactSubscription

- payment_revenue

## FactAICost

- ai_response_cost

## FactUserActivity

- daily_active_users
- total_sessions
- total_events

## FactExecutive

- total_users
- total_sessions
- total_events
- total_revenue
- total_tokens
- avg_latency_ms
- success_rate_percent
- avg_events_per_user
- avg_sessions_per_user
- avg_tokens_per_event
- avg_revenue_per_event

---

# 7. Current Spark Gold Datasets

- country_metrics
- executive_dashboard
- feedback_metrics
- model_performance
- prompt_metrics
- revenue_metrics
- session_metrics
- subscription_metrics
- user_activity

Format:

Parquet

---

# 8. Current Architecture

Python Producer
    |
    v
Kafka
    |
    v
Spark Structured Streaming
    |
    +---- Bronze
    |
    +---- Silver
    |
    +---- Gold
             |
             v
       Gold Parquet
             |
             v
          Power BI

---

# 9. Planned Cloud Architecture

Python Producer
    |
    v
Kafka
    |
    v
Spark
    |
    v
Bronze
    |
    v
Silver
    |
    v
Gold
    |
    v
AWS S3
    |
    v
AWS Glue Data Catalog
    |
    v
Amazon Athena
    |
    v
dbt
    |
    v
Analytics / Mart Layer
    |
    v
Power BI

Airflow will orchestrate the workflow.

---

# 10. Known Power BI Relationships

The following relationships have previously been identified and MUST be verified against the actual baseline PBIX before cloud migration:

- DimDate -> FactAICost
- DimDate -> FactUserActivity
- DimCountry -> FactCountry
- DimModel -> FactModel
- DimPrompt -> FactPrompt
- DimPlan -> FactSession
- DimPlan -> FactSubscription

IMPORTANT:

This is a preliminary relationship list.
It is NOT yet the final verified relationship contract.

---

# 11. Important DAX Definitions

Known:

Total Revenue =
SUM(FactSubscription[payment_revenue])

AI Cost =
SUM(FactAICost[ai_response_cost])

Gross Contribution =
Payment Revenue - AI Cost

Other known measures include:

- Total Requests
- Total Tokens
- Total Active Users
- Total Sessions
- Total Events
- Average Latency
- Success Rate
- AI Cost % of Revenue
- Net Revenue after AI Cost

IMPORTANT:

Exact DAX expressions must be verified from the baseline PBIX.

---

# 12. Power BI Semantic Model Contract

The cloud layer MUST preserve compatibility with the existing Power BI model.

Required logical tables:

FactExecutive
FactUserActivity
FactAICost
FactSubscription
FactCountry
FactModel
FactPrompt
FactFeedback
FactSession

DimDate
DimPlan
DimCountry
DimModel
DimPrompt

_Measures

---

# 13. Compatibility Requirements

The following should remain stable during migration:

- Table names
- Column names
- Required columns
- Data types
- Measure names
- Measure definitions
- Relationships
- Dimension structure
- Fact structure
- Filter behavior

---

# 14. Power BI Safety Rules

1. Do not delete the baseline PBIX.
2. Do not rename Power BI tables.
3. Do not rename required columns.
4. Do not change required data types without validation.
5. Do not unnecessarily modify DAX measures.
6. Do not delete existing relationships.
7. Do not rebuild report pages.
8. Do not rebuild navigation.
9. Do not delete existing Gold Parquet files.
10. Test cloud migration only in the Cloud Test PBIX.

---

# 15. Current Power BI Data Source

Current:

Gold Parquet datasets

Current path concept:

Local Spark Gold
    |
    v
Parquet
    |
    v
Power BI

---

# 16. Future Power BI Data Source

Potential future architecture:

S3
    |
    v
Glue
    |
    v
Athena
    |
    v
dbt Analytics Layer
    |
    v
Power BI

IMPORTANT:

Power BI source will NOT be changed until cloud data has been fully validated.

---

# 17. Migration Validation

The following must match between the current Gold layer and cloud/dbt layer:

- Total Users
- Total Sessions
- Total Events
- Total Revenue
- Total Tokens
- Total AI Cost
- Average Latency
- Success Rate

Validation status:

PENDING

---

# 18. Power BI Baseline

Baseline PBIX:

AI-SaaS-Analytics_POWERBI_BASELINE.pbix

Status:

DO NOT MODIFY

---

# 19. Power BI Cloud Test

Cloud test PBIX:

AI-SaaS-Analytics_POWERBI_CLOUD_TEST.pbix

Status:

SAFE FOR CLOUD TESTING

---

# 20. Migration Strategy

Phase 1:
Keep existing Power BI connected to local Gold Parquet.

Phase 2:
Copy Gold data to AWS S3.

Phase 3:
Create Glue Catalog.

Phase 4:
Query S3 through Athena.

Phase 5:
Build dbt models and tests.

Phase 6:
Validate cloud/dbt metrics against current Gold.

Phase 7:
Test cloud connection in separate Power BI PBIX.

Phase 8:
Validate tables, columns, data types, measures, relationships and visuals.

Phase 9:
Only after successful validation consider changing the Power BI source.

---

# 21. Final Principle

POWER BI IS THE KNOWN-GOOD BASELINE.

The cloud pipeline must adapt to the existing analytical contract wherever possible.

Do not modify the dashboard simply to accommodate the cloud pipeline.

---

# 22. Contract Status

Version: 1.0

Created: 2026-09-07

Status:

INITIAL BASELINE CREATED

Next task:

EXTRACT AND VERIFY THE EXACT POWER BI SEMANTIC MODEL.

This includes:

- Exact tables
- Exact columns
- Exact data types
- Exact relationships
- Exact cardinality
- Exact filter direction
- Exact measures
- Exact DAX expressions

After verification:

POWER BI = FROZEN

Then:

AWS → S3 → Glue → Athena → dbt → Airflow

will be built without modifying the baseline.
