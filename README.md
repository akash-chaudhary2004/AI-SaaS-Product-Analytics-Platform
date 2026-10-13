# AI SaaS Product Analytics Platform

An end-to-end analytics engineering project that transforms synthetic AI SaaS telemetry into business-ready insights using AWS, dbt, and Power BI.

## Architecture

**Python → Amazon S3 → AWS Glue → Amazon Athena → dbt → Power BI**

## Tech Stack

- **Programming:** Python, SQL
- **AWS:** Amazon S3, AWS Glue Data Catalog, Amazon Athena
- **Analytics Engineering:** dbt Core, dbt-athena
- **Business Intelligence:** Power BI, DAX, Power Query
- **Tools:** Git, GitHub

## Analytics Marts

The platform provides nine analytics marts covering:

- Executive KPIs and user activity
- Revenue and subscriptions
- AI model performance and prompt usage
- Country-level metrics
- Sessions and user feedback

## Validation Results

- 18 dbt models discovered: 9 staging and 9 mart models
- All 9 configured dbt data-quality tests passed
- All 9 Athena marts successfully checked for row counts
- Nine Power BI Athena-connected queries refreshed successfully in the verified cloud-connected copy

*Validation performed on October 11, 2026. Results may change as the underlying data changes.*

## Project Structure

```text
AI-SaaS-Product-Analytics-Platform/
|-- config/
|-- dashboards/
|-- data/
|-- dbt/
|   `-- ai_saas_analytics/
|       |-- models/
|       |   |-- staging/
|       |   `-- marts/
|       `-- requirements.txt
|-- documentation/
|-- generators/
|-- check_marts.py
|-- requirements.txt
`-- README.md
```

## Getting Started

**Prerequisites:** Python 3.11, AWS CLI, Git, and access to the required AWS resources.

1. Clone the repository:

   ```powershell
   git clone <YOUR_GITHUB_REPOSITORY_URL>
   cd AI-SaaS-Product-Analytics-Platform
   ```

2. Create and activate a virtual environment:

   ```powershell
   python -m venv .venv-dbt
   .\.venv-dbt\Scripts\Activate.ps1
   ```

3. Install dependencies:

   ```powershell
   python -m pip install -r dbt\ai_saas_analytics\requirements.txt
   ```

4. Configure AWS authentication and your dbt profile at `%USERPROFILE%\.dbt\profiles.yml`.

5. Validate the project:

   ```powershell
   dbt parse --project-dir dbt\ai_saas_analytics
   dbt test --project-dir dbt\ai_saas_analytics
   python check_marts.py
   ```

*Note: AWS resources and permissions must be configured before running cloud validations. Athena queries may incur charges. Automated event-to-S3 ingestion from a fresh clone has not yet been verified.*

## Future Improvements

- Automate ingestion and pipeline scheduling
- Expand business-logic and relationship tests
- Add CI/CD and pipeline monitoring
- Document dashboard insights and cloud costs

## Skills Demonstrated

Python, SQL, dbt, data modeling, data quality testing, AWS S3, AWS Glue, Amazon Athena, Power BI, DAX, and Power Query.

## Author

**Akash Kumar**

B.Tech in Smart Manufacturing

PDPM, Indian Institute of Information Technology, Jabalpur