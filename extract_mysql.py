"""
ingestion/extract_mysql.py
--------------------------
Extracts shipment transactions and lane rates from MySQL.
Returns pandas DataFrames ready for loading into Snowflake.
"""

import yaml
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from utils.logger import get_logger

log = get_logger("extract_mysql")


def load_config(config_path: str = "config/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_mysql_engine(cfg: dict):
    mysql = cfg["mysql"]
    url = (
        f"mysql+mysqlconnector://{mysql['user']}:{mysql['password']}"
        f"@{mysql['host']}:{mysql['port']}/{mysql['database']}"
    )
    engine = create_engine(url, pool_pre_ping=True)
    log.info(f"Connected to MySQL: {mysql['host']}/{mysql['database']}")
    return engine


def extract_shipments(engine, days_back: int = 30) -> pd.DataFrame:
    """Extract express parcel shipments from the last N days."""
    cutoff_date = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

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
          AND shipment_date >= :cutoff
        ORDER BY shipment_date
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"cutoff": cutoff_date})

    log.info(f"Extracted {len(df):,} shipment rows (since {cutoff_date})")
    return df


def extract_lane_rates(engine) -> pd.DataFrame:
    """Extract all active lane rate slabs."""
    query = text("""
        SELECT
            lane_code,
            weight_slab_min,
            weight_slab_max,
            rate_per_kg,
            fuel_surcharge_pct,
            effective_date
        FROM lane_rates
        WHERE effective_date <= CURDATE()
        ORDER BY lane_code, weight_slab_min, effective_date DESC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    log.info(f"Extracted {len(df):,} lane rate rows")
    return df


def run_extraction(config_path: str = "config/config.yaml") -> dict:
    cfg = load_config(config_path)
    engine = get_mysql_engine(cfg)
    days_back = cfg["pipeline"].get("extract_days_back", 30)

    shipments_df = extract_shipments(engine, days_back=days_back)
    lane_rates_df = extract_lane_rates(engine)

    # Basic validation
    assert not shipments_df.empty, "No shipments extracted — check date range or filters"
    assert not lane_rates_df.empty, "No lane rates found — check lane_rates table"

    log.info("Extraction complete ✓")
    return {
        "shipments": shipments_df,
        "lane_rates": lane_rates_df,
    }


if __name__ == "__main__":
    data = run_extraction()
    print(data["shipments"].head())
    print(data["lane_rates"].head())
