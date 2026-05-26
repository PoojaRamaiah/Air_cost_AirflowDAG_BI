"""
ingestion/incremental_extract.py
----------------------------------
Incremental extraction from MySQL using a watermark table in Snowflake.

Strategy:
  - On first run: full load (last 90 days)
  - On subsequent runs: extract only rows newer than the last loaded timestamp
  - Watermark is stored in Snowflake: LOGISTICS_DW.RAW.ETL_WATERMARKS

This prevents re-loading historical data on every pipeline run.
"""

import yaml
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import snowflake.connector
from utils.logger import get_logger

log = get_logger("incremental_extract")

WATERMARK_TABLE = "LOGISTICS_DW.RAW.ETL_WATERMARKS"
FULL_LOAD_LOOKBACK_DAYS = 90


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Watermark helpers ─────────────────────────────────────────────────────────

def get_snowflake_conn(cfg: dict):
    sf = cfg["snowflake"]
    return snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=sf["password"],
        role=sf["role"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"],
    )


def ensure_watermark_table(sf_conn):
    sf_conn.cursor().execute(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
            table_name      VARCHAR(100) PRIMARY KEY,
            last_loaded_at  TIMESTAMP_NTZ,
            last_row_count  INTEGER,
            updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    log.info("Watermark table verified ✓")


def get_watermark(sf_conn, table_name: str) -> datetime | None:
    """Returns the last loaded timestamp for a table, or None if first run."""
    cursor = sf_conn.cursor()
    cursor.execute(
        f"SELECT last_loaded_at FROM {WATERMARK_TABLE} WHERE table_name = %s",
        (table_name,)
    )
    row = cursor.fetchone()
    if row and row[0]:
        log.info(f"Watermark for '{table_name}': {row[0]}")
        return row[0]
    log.info(f"No watermark found for '{table_name}' — will do full load")
    return None


def set_watermark(sf_conn, table_name: str, loaded_at: datetime, row_count: int):
    """Upsert watermark after a successful load."""
    sf_conn.cursor().execute(f"""
        MERGE INTO {WATERMARK_TABLE} AS target
        USING (SELECT %s AS table_name, %s::TIMESTAMP_NTZ AS last_loaded_at, %s AS last_row_count) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            last_loaded_at = source.last_loaded_at,
            last_row_count = source.last_row_count,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (table_name, last_loaded_at, last_row_count)
            VALUES (source.table_name, source.last_loaded_at, source.last_row_count)
    """, (table_name, loaded_at.isoformat(), row_count))
    log.info(f"Watermark updated for '{table_name}': {loaded_at} ({row_count:,} rows)")


# ── Incremental extractors ────────────────────────────────────────────────────

def get_mysql_engine(cfg: dict):
    mysql = cfg["mysql"]
    url = (
        f"mysql+mysqlconnector://{mysql['user']}:{mysql['password']}"
        f"@{mysql['host']}:{mysql['port']}/{mysql['database']}"
    )
    return create_engine(url, pool_pre_ping=True)


def extract_shipments_incremental(mysql_engine, watermark: datetime | None) -> pd.DataFrame:
    """
    If watermark exists: extract rows created after the last load.
    If no watermark: full load from last FULL_LOAD_LOOKBACK_DAYS days.
    """
    if watermark:
        cutoff = watermark
        log.info(f"Incremental load: shipments since {cutoff}")
    else:
        cutoff = datetime.now() - timedelta(days=FULL_LOAD_LOOKBACK_DAYS)
        log.info(f"Full load: shipments since {cutoff.date()} ({FULL_LOAD_LOOKBACK_DAYS} days)")

    query = text("""
        SELECT
            shipment_id,
            origin_city,
            destination_city,
            lane_code,
            service_type,
            actual_weight_kg,
            volumetric_weight_kg,
            chargeable_weight_kg,
            shipment_date,
            created_at
        FROM shipments
        WHERE service_type = 'EXPRESS'
          AND created_at > :cutoff
        ORDER BY created_at
    """)

    with mysql_engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"cutoff": cutoff})

    log.info(f"Extracted {len(df):,} shipment rows (incremental)")
    return df


def extract_lane_rates_incremental(mysql_engine, watermark: datetime | None) -> pd.DataFrame:
    """
    Lane rates are a small reference table — always do a full refresh
    to pick up any rate changes, but only if updated since last run.
    """
    if watermark:
        query = text("""
            SELECT lane_code, weight_slab_min, weight_slab_max,
                   rate_per_kg, fuel_surcharge_pct, effective_date
            FROM lane_rates
            WHERE effective_date >= :since
            ORDER BY lane_code, weight_slab_min
        """)
        since = watermark.date()
    else:
        query = text("""
            SELECT lane_code, weight_slab_min, weight_slab_max,
                   rate_per_kg, fuel_surcharge_pct, effective_date
            FROM lane_rates
            ORDER BY lane_code, weight_slab_min
        """)
        since = None

    with mysql_engine.connect() as conn:
        params = {"since": since} if since else {}
        df = pd.read_sql(query, conn, params=params)

    log.info(f"Extracted {len(df):,} lane rate rows")
    return df


# ── Snowflake MERGE load (upsert) ─────────────────────────────────────────────

def upsert_shipments(sf_conn, df: pd.DataFrame):
    """
    Uses Snowflake MERGE to upsert shipments by shipment_id.
    Prevents duplicates on re-runs or overlapping windows.
    """
    if df.empty:
        log.info("No new shipments to upsert")
        return

    df.columns = [c.upper() for c in df.columns]

    # Write to a temp stage table first
    from snowflake.connector.pandas_tools import write_pandas
    write_pandas(sf_conn, df, "RAW_SHIPMENTS_STAGE",
                 database="LOGISTICS_DW", schema="RAW",
                 auto_create_table=True, overwrite=True)

    # MERGE into main table
    sf_conn.cursor().execute("""
        MERGE INTO LOGISTICS_DW.RAW.RAW_SHIPMENTS AS target
        USING LOGISTICS_DW.RAW.RAW_SHIPMENTS_STAGE AS source
        ON target.SHIPMENT_ID = source.SHIPMENT_ID
        WHEN MATCHED THEN UPDATE SET
            CHARGEABLE_WEIGHT_KG = source.CHARGEABLE_WEIGHT_KG,
            SHIPMENT_DATE        = source.SHIPMENT_DATE,
            _LOADED_AT           = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            SHIPMENT_ID, ORIGIN_CITY, DESTINATION_CITY, LANE_CODE,
            SERVICE_TYPE, ACTUAL_WEIGHT_KG, VOLUMETRIC_WEIGHT_KG,
            CHARGEABLE_WEIGHT_KG, SHIPMENT_DATE, CREATED_AT, _LOADED_AT
        ) VALUES (
            source.SHIPMENT_ID, source.ORIGIN_CITY, source.DESTINATION_CITY,
            source.LANE_CODE, source.SERVICE_TYPE, source.ACTUAL_WEIGHT_KG,
            source.VOLUMETRIC_WEIGHT_KG, source.CHARGEABLE_WEIGHT_KG,
            source.SHIPMENT_DATE, source.CREATED_AT, CURRENT_TIMESTAMP()
        )
    """)
    log.info(f"Upserted {len(df):,} shipments into RAW_SHIPMENTS ✓")


# ── Main incremental runner ───────────────────────────────────────────────────

def run_incremental(config_path: str = "config/config.yaml"):
    cfg = load_config(config_path)
    sf_conn = get_snowflake_conn(cfg)
    mysql_engine = get_mysql_engine(cfg)

    ensure_watermark_table(sf_conn)

    # Get watermarks
    shipment_wm = get_watermark(sf_conn, "raw_shipments")
    rates_wm    = get_watermark(sf_conn, "raw_lane_rates")

    run_start = datetime.utcnow()

    # Extract
    shipments_df  = extract_shipments_incremental(mysql_engine, shipment_wm)
    lane_rates_df = extract_lane_rates_incremental(mysql_engine, rates_wm)

    # Load (upsert)
    upsert_shipments(sf_conn, shipments_df)

    # Update watermarks
    if not shipments_df.empty:
        set_watermark(sf_conn, "raw_shipments", run_start, len(shipments_df))
    if not lane_rates_df.empty:
        set_watermark(sf_conn, "raw_lane_rates", run_start, len(lane_rates_df))

    sf_conn.close()
    log.info("Incremental run complete ✓")
    return {"shipments": shipments_df, "lane_rates": lane_rates_df}


if __name__ == "__main__":
    run_incremental()
