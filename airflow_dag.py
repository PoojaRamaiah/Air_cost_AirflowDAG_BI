"""
orchestration/airflow_dag.py
-----------------------------
Airflow DAG for the Air Cost Lane ETL pipeline.

Schedule: Daily at 2 AM IST (20:30 UTC previous day)
Tasks:
  1. extract_mysql        → pull shipments from MySQL
  2. load_snowflake       → bulk load into Snowflake RAW
  3. dbt_staging          → run staging models
  4. dbt_marts            → run mart models
  5. dbt_test             → run dbt data quality tests
  6. notify_success       → log completion (extend with Slack/email)

Install extras:
  pip install apache-airflow apache-airflow-providers-snowflake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["de-alerts@yourcompany.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="air_cost_lane_etl",
    description="MySQL → Snowflake → dbt: Air cost lane-wise for express parcels",
    default_args=default_args,
    schedule_interval="30 20 * * *",   # 2:00 AM IST daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "air-cost", "snowflake", "dbt"],
) as dag:

    # ── Task 1: Extract from MySQL ────────────────────────────────────────────

    def task_extract(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/dags/air_cost_lane_etl")
        from ingestion.extract_mysql import run_extraction

        config_path = Variable.get("air_cost_config_path", default_var="config/config.yaml")
        data = run_extraction(config_path)

        # Push to XCom as record counts (not full DataFrames — use temp files for large data)
        context["ti"].xcom_push(key="shipment_count", value=len(data["shipments"]))
        context["ti"].xcom_push(key="lane_rate_count", value=len(data["lane_rates"]))

        # Persist DataFrames to temp parquet for next task
        data["shipments"].to_parquet("/tmp/air_cost_shipments.parquet", index=False)
        data["lane_rates"].to_parquet("/tmp/air_cost_lane_rates.parquet", index=False)

        return len(data["shipments"])

    extract_task = PythonOperator(
        task_id="extract_mysql",
        python_callable=task_extract,
        provide_context=True,
    )

    # ── Task 2: Short circuit if no new data ─────────────────────────────────

    def check_has_data(**context):
        count = context["ti"].xcom_pull(task_ids="extract_mysql")
        if count == 0:
            print("No new shipments found — skipping downstream tasks.")
            return False
        print(f"Found {count:,} shipments — proceeding.")
        return True

    check_data_task = ShortCircuitOperator(
        task_id="check_has_data",
        python_callable=check_has_data,
        provide_context=True,
    )

    # ── Task 3: Load into Snowflake ───────────────────────────────────────────

    def task_load(**context):
        import sys
        import pandas as pd
        sys.path.insert(0, "/opt/airflow/dags/air_cost_lane_etl")
        from ingestion.load_snowflake import run_load

        config_path = Variable.get("air_cost_config_path", default_var="config/config.yaml")
        data = {
            "shipments": pd.read_parquet("/tmp/air_cost_shipments.parquet"),
            "lane_rates": pd.read_parquet("/tmp/air_cost_lane_rates.parquet"),
        }
        run_load(data, config_path)

    load_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=task_load,
        provide_context=True,
    )

    # ── Task 4: dbt staging models ────────────────────────────────────────────

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            "cd /opt/airflow/dags/air_cost_lane_etl/dbt_project && "
            "dbt run --select staging --profiles-dir . --target prod"
        ),
    )

    # ── Task 5: dbt mart models ───────────────────────────────────────────────

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            "cd /opt/airflow/dags/air_cost_lane_etl/dbt_project && "
            "dbt run --select marts --profiles-dir . --target prod"
        ),
    )

    # ── Task 6: dbt tests ─────────────────────────────────────────────────────

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dags/air_cost_lane_etl/dbt_project && "
            "dbt test --profiles-dir . --target prod"
        ),
    )

    # ── Task 7: Notify success ────────────────────────────────────────────────

    def notify_success(**context):
        shipment_count = context["ti"].xcom_pull(task_ids="extract_mysql")
        run_date = context["ds"]
        print(f"✅ Pipeline completed for {run_date} | {shipment_count:,} shipments processed")
        # Extend: send Slack message, trigger BI refresh, write audit log, etc.

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    extract_task >> check_data_task >> load_task >> dbt_staging >> dbt_marts >> dbt_test >> notify_task
