## ✈️ Air Cost Lane-wise ETL Pipeline

A production-ready data engineering pipeline that calculates **lane-wise air freight costs** for express parcel shipments.

Extracts shipment transactions from **MySQL**, loads them into **Snowflake**, and runs **dbt** SQL models to compute base cost, fuel surcharge, and total air cost per lane — broken down by weight slab, month, and route.

### What this project does
- Pulls express parcel shipments from a MySQL OLTP database
- Applies chargeable weight logic: `max(actual_weight, volumetric_weight)`
- Joins each shipment to its applicable rate slab by lane code and weight band
- Calculates base cost + fuel surcharge per shipment
- Aggregates lane-wise air cost by month with MoM trend, rolling averages, and cost rankings
- Exposes 5 BI-ready Snowflake views for dashboards (Tableau / Power BI / Metabase)
- Runs daily via **Airflow** with incremental loading and watermark-based deduplication

### Tech stack
| Layer | Tools |
|---|---|
| Source | MySQL 8.x |
| Warehouse | Snowflake |
| Ingestion | Python 3.10, SQLAlchemy, snowflake-connector-python |
| Transformation | dbt Core + dbt-snowflake |
| Orchestration | Apache Airflow |
| Testing | pytest, dbt tests |

### Key output: `mart_air_cost_by_lane`
Lane-level monthly aggregation with total shipments, chargeable kg, base cost, fuel surcharge, total air cost, effective rate/kg, and MoM cost growth — ready to connect to any BI tool.
