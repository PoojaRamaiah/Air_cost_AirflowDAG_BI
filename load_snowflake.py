"""
ingestion/load_snowflake.py
---------------------------
Loads extracted DataFrames into Snowflake RAW schema tables.
Creates tables if they don't exist, then does MERGE / INSERT.
"""

import yaml
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from utils.logger import get_logger

log = get_logger("load_snowflake")


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_snowflake_conn(cfg: dict):
    sf = cfg["snowflake"]
    conn = snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=sf["password"],
        role=sf["role"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"],
    )
    log.info(f"Connected to Snowflake: {sf['database']}.{sf['schema']}")
    return conn


def ensure_tables(conn):
    """Create RAW tables in Snowflake if they don't exist."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.RAW_SHIPMENTS (
            SHIPMENT_ID             VARCHAR(50),
            ORIGIN_CITY             VARCHAR(100),
            DESTINATION_CITY        VARCHAR(100),
            LANE_CODE               VARCHAR(20),
            SERVICE_TYPE            VARCHAR(20),
            ACTUAL_WEIGHT_KG        NUMBER(10,3),
            VOLUMETRIC_WEIGHT_KG    NUMBER(10,3),
            CHARGEABLE_WEIGHT_KG    NUMBER(10,3),
            SHIPMENT_DATE           DATE,
            CREATED_AT              TIMESTAMP_NTZ,
            _LOADED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.RAW_LANE_RATES (
            LANE_CODE               VARCHAR(20),
            WEIGHT_SLAB_MIN         NUMBER(10,3),
            WEIGHT_SLAB_MAX         NUMBER(10,3),
            RATE_PER_KG             NUMBER(10,4),
            FUEL_SURCHARGE_PCT      NUMBER(5,2),
            EFFECTIVE_DATE          DATE,
            _LOADED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    cursor.close()
    log.info("RAW tables verified ✓")


def load_dataframe(conn, df: pd.DataFrame, table_name: str, batch_size: int = 10000):
    """Write a DataFrame to a Snowflake table using write_pandas (bulk load)."""
    # Snowflake expects uppercase column names
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table_name,
        database=conn.database,
        schema="RAW",
        chunk_size=batch_size,
        auto_create_table=False,
        overwrite=False,
    )

    if success:
        log.info(f"Loaded {nrows:,} rows into RAW.{table_name} ({nchunks} chunks)")
    else:
        raise RuntimeError(f"Failed to load data into {table_name}")


def run_load(data: dict, config_path: str = "config/config.yaml"):
    cfg = load_config(config_path)
    batch_size = cfg["pipeline"].get("batch_size", 10000)

    conn = get_snowflake_conn(cfg)
    ensure_tables(conn)

    load_dataframe(conn, data["shipments"], "RAW_SHIPMENTS", batch_size)
    load_dataframe(conn, data["lane_rates"], "RAW_LANE_RATES", batch_size)

    conn.close()
    log.info("Load complete ✓")


if __name__ == "__main__":
    from ingestion.extract_mysql import run_extraction
    data = run_extraction()
    run_load(data)
