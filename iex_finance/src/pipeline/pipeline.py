import os 
from database.postgres import PostgresDB
from etl.transform import Transform
from etl.extract import Extract
from etl.load import Load
import yaml 
from io import StringIO
import logging
from utility.metadata_logging import MetadataLogging
import datetime as dt
import pandas as pd
def pipeline()->bool:
    
    # set up logging 
    run_log = StringIO()
    logging.basicConfig(stream=run_log,level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")
        
    # set up metadata logger 
    metadata_logger = MetadataLogging(db_target="target")
    with open("config.yaml") as stream:
        config = yaml.safe_load(stream)
        
    metadata_log_table = config["meta"]["log_table"]
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_log_run_id, 
        run_config=config,
        db_table=metadata_log_table
    )
    logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s", level=logging.INFO) # format: https://docs.python.org/3/library/logging.html#logging.LogRecord
    iex_api_key=os.environ.get("iex_api_key")
        
    logging.info("Commencing extract")
    # extract 
    df = Extract.extract_stocks(iex_api_key)
    logging.info("Extract complete")
    logging.info("Commencing extract from stock csv")
    logging.info("Extract complete")

    # transform 
    logging.info("Commencing transform")
    df_transformed = Transform.transform(df)
    logging.info("Transform complete")
    
    #load file 
    Load.load(
        df=df_transformed,
        load_target="file",
        target_file_directory="data",
        target_file_name="stock.parquet",
    )
    logging.info("File load complete")

    engine = PostgresDB.create_pg_engine()
    
    # load database (upsert)
    logging.info("Commencing database load")
    Load.load(
        df=df_transformed,
        load_target="database",
        target_database_engine=engine,
        target_table_name="stocks_intraday"
    )  
    logging.info("Database load complete")
    df_staged = Transform.transform_staging(model="staging_stock", engine=engine, models_path="models/transform")
    
    logging.info("staging_stock transformed from stocks_intraday")

    logging.info("Pipeline run successful")
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="completed",
        run_id=metadata_log_run_id, 
        run_config=config,
        run_log=run_log.getvalue(),
        db_table=metadata_log_table
    )
    print(run_log.getvalue())
    
if __name__ == "__main__":
    pipeline()