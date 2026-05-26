"""
orchestration/run_pipeline.py
------------------------------
End-to-end pipeline runner.
  1. Extract from MySQL
  2. Load to Snowflake RAW
  3. Run dbt transformations

Run:
    python orchestration/run_pipeline.py
    python orchestration/run_pipeline.py --config config/config.yaml --dbt-only
"""

import argparse
import subprocess
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import get_logger

log = get_logger("pipeline")


def run_extraction_and_load(config_path: str):
    from ingestion.extract_mysql import run_extraction
    from ingestion.load_snowflake import run_load

    log.info("─── STEP 1: Extracting from MySQL ───")
    data = run_extraction(config_path)

    log.info("─── STEP 2: Loading into Snowflake ───")
    run_load(data, config_path)


def run_dbt(dbt_project_dir: str = "dbt_project"):
    log.info("─── STEP 3: Running dbt transformations ───")

    commands = [
        ["dbt", "deps"],
        ["dbt", "run", "--select", "staging"],
        ["dbt", "run", "--select", "marts"],
        ["dbt", "test"],
    ]

    for cmd in commands:
        log.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=dbt_project_dir,
            capture_output=False,
        )
        if result.returncode != 0:
            log.error(f"dbt command failed: {' '.join(cmd)}")
            raise RuntimeError(f"dbt step failed: {cmd}")

    log.info("dbt transformations complete ✓")


def main():
    parser = argparse.ArgumentParser(description="Air Cost Lane ETL Pipeline")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--dbt-only", action="store_true", help="Skip extract/load, run dbt only")
    parser.add_argument("--extract-only", action="store_true", help="Only run extract + load")
    args = parser.parse_args()

    start = datetime.now()
    log.info(f"Pipeline started at {start.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        if not args.dbt_only:
            run_extraction_and_load(args.config)

        if not args.extract_only:
            run_dbt()

        elapsed = (datetime.now() - start).seconds
        log.info(f"✅ Pipeline completed successfully in {elapsed}s")

    except Exception as e:
        log.error(f"❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
