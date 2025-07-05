import os
import yaml
import logging
from io import StringIO
import datetime as dt
import pandas as pd

from database.postgres import PostgresDB
from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load
from utility.metadata_logging import MetadataLogging

def pipeline() -> bool:
    """
    Main ETL pipeline to extract, transform, and load stock intraday data.
    Logs progress and metadata about the run.
    
    Returns:
        bool: True if pipeline completes successfully
    """

    # Basic logging configuration with log capture stream and initial format
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s][%(asctime)s]: %(message)s"
    )

    # Initialize metadata logger for tracking pipeline runs
    metadata_logger = MetadataLogging()
    metadata_log_table = "pipeline_log"

    # Get the latest run ID for logging continuity
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)

    # Log start of this run in metadata table
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_log_run_id,
        db_table=metadata_log_table
    )

    # Read API key for data extraction from environment variables (stored in S3, grant permission to ECS)
    iex_api_key = os.environ.get("iex_api_key")

    # Extract phase
    logging.info("Commencing extract")
    df = Extract.extract_stocks(iex_api_key)
    logging.info("Extract complete")


    # Transform phase
    logging.info("Commencing transform")
    df_transformed = Transform.transform(df)
    logging.info("Transform complete")


    # Create a database connection engine
    engine = PostgresDB.create_pg_engine()

    # Load transformed data into the database with upsert logic
    logging.info("Commencing database load")
    Load.load(
        df=df_transformed,
        load_target="database",
        target_database_engine=engine,
        target_table_name="stocks_intraday"
    )
    logging.info("Database load complete")

    # Log successful completion in metadata table, including run logs
    logging.info("Pipeline run successful")
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="completed",
        run_id=metadata_log_run_id,
        db_table=metadata_log_table
    )

    return True

if __name__ == "__main__":
    pipeline()
