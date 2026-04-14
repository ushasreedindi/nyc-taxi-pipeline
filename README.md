# NYC Taxi Data Pipeline 🚕

End-to-end cloud ELT pipeline processing 10.8M+ NYC taxi trips.

## Architecture
Raw Data → Python → Snowflake (RAW) → dbt → Snowflake (STAGING/MART) → Power BI

## Tech Stack
- **Python** — data ingestion and cleaning
- **Snowflake** — cloud data warehouse
- **dbt** — SQL transformations and data quality tests
- **Power BI** — dashboard and visualization
- **GitHub** — version control

## Pipeline Layers
| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| Raw | `RAW.TAXI_TRIPS` | 10.8M | Raw ingested data |
| Staging | `STAGING.STG_TAXI_TRIPS` | 10.8M | Cleaned & transformed |
| Mart | `MART.MART_DAILY_SUMMARY` | 744 | Aggregated analytics |

## dbt Tests
- 13 automated data quality tests
- not_null checks on all key columns
- accepted_values validation on payment_type

## Key Findings
- Processed 10,837,367 NYC taxi trips (January 2016)
- Average fare: $12.49
- Average trip distance: 4.65 miles
- 744 hourly summary records built for analytics
