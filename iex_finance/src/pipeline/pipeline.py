import os
import logging
from io import StringIO
import datetime as dt
from database.postgres import PostgresDB
from etl.extract import Extract, Transform, Load
from utility.metadata_logging import MetadataLogging

def pipeline() -> bool:
    """
    Main ETL pipeline to extract, transform, and load stock intraday data.
    Logs progress and metadata about the run.

    Returns:
        bool: True if pipeline completes successfully, False otherwise.
    """

    # Set up in-memory log capture
    run_log = StringIO()
    logging.basicConfig(
        stream=run_log,
        level=logging.INFO,
        format="[%(levelname)s][%(asctime)s]: %(message)s"
    )

    # Initialize metadata logger and setup log table
    metadata_logger = MetadataLogging()
    metadata_log_table = "pipeline_log"

    # Get latest run ID for continuity
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)

    # Log pipeline start
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_log_run_id,
        db_table=metadata_log_table
    )

    # Read API key for extraction (stored in S3, grant access to ECS)
    iex_api_key = os.environ.get("iex_api_key")
    if not iex_api_key:
        logging.error("Missing IEX API key in environment variables.")
        raise EnvironmentError("IEX API key is required but not set.")

    # Extract phase (daily data)
    logging.info("Commencing extract")
    df = Extract.extract_stocks(iex_api_key)
    logging.info("Extract complete")

    # Transform phase: Pandas
    logging.info("Commencing Pandas transform")
    df_transformed = Transform.transform_pandas(df)
    logging.info("Pandas transform complete")

    # Load phase
    engine = PostgresDB.create_pg_engine()
    logging.info("Commencing database load")
    Load.load(
        df=df_transformed,
        target_database_engine=engine,
        target_table_name="stocks_intraday"
    )
    logging.info("Database load complete")

    # Weekly stock aggregation
    Transform.weekly_stocl_agg(
        model="weekly_stock_agg",  # points to weekly_stock_agg.sql
        engine=engine,
        models_path="./models/transform",
        target_table="stocks_weekly"
    )
    logging.info("Weekly stock aggregation complete")

    # Log successful completion
    logging.info("Pipeline run successful")
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="completed",
        run_id=metadata_log_run_id,
        run_log=run_log.getvalue(),
        db_table=metadata_log_table
    )

    return True

if __name__ == "__main__":
    pipeline()
