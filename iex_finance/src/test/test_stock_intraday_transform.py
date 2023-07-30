from database.postgres import PostgresDB
import pandas as pd 
import os 
import jinja2 as j2 
from etl.transform import Transform
def test_stocks_intraday():
    # -- assemble -- 
    actual_source_stocks_intraday = "stocks_intraday"
    mock_source_stocks_intraday = f"mock_{actual_source_stocks_intraday}"
    actual_target_table="stocks_intraday"
    mock_target_table= f"mock_{actual_target_table}"
    path_model_transform = "models/transform"
    engine = PostgresDB.create_pg_engine()
            
    # create mock staging_orders
    df_mock_data_staging_stock = pd.DataFrame({
        "stock_code":["AAPL","TSLA"],
        "max_open_value_per_day":[151.165,205.59],
        "min_open_value_per_day":[149.39,193.18],
        "max_close_value_per_day":[151.225,205.61],
        "min_close_value_per_day":[149.41,193.225],
        "max_high_per_day":[151.225,206.2],
        "min_high_per_day":[149.44,193.4],
        "max_low_per_day":[151.01,205.07],
        "min_low_per_day":[149.26,193.03],
        "status_difference":[-1.1100000000000136,9.055000000000291],
        "trades_mean":[14.592261904761905,20.246290801186944],
        "volume_mean":[2095.535714285714,2442.1364985163204]
    })
    df_mock_data_staging_stock.to_sql(name=mock_source_stocks_intraday, con=engine, if_exists="replace", index=False)


    # get transform sql file 
    with open(f"{path_model_transform}/{actual_target_table}.sql") as f: 
        raw_sql = f.read()
        
    raw_sql = raw_sql.replace(actual_source_stocks_intraday, mock_source_stocks_intraday)
    # write mock transform file 
    with open(f"{path_model_transform}/{mock_target_table}.sql", mode="w") as f:  
            f.write(raw_sql)
    df_expected_output=pd.DataFrame({
        "stock_code":["AAPL","TSLA"],
        "max_open_value_per_day":[151.165,205.59],
        "min_open_value_per_day":[149.39,193.18],
        "max_close_value_per_day":[151.225,205.61],
        "min_close_value_per_day":[149.41,193.225],
        "max_high_per_day":[151.225,206.2],
        "min_high_per_day":[149.44,193.4],
        "max_low_per_day":[151.01,205.07],
        "min_low_per_day":[149.26,193.03],
        "status_difference":[-1.1100000000000136,9.055000000000291],
        "trades_mean":[14.592261904761905,20.246290801186944],
        "volume_mean":[2095.535714285714,2442.1364985163204]  
    })


    # -- act -- 
    staging_stocks= Transform.transform_staging(mock_target_table, engine=engine, models_path=path_model_transform)
    df_output = pd.read_sql(mock_target_table,con=engine)
    # clean up first 
    engine.execute(f"drop table {mock_source_stocks_intraday}")


    # -- assert -- 
    pd.testing.assert_frame_equal(left=df_output, right=df_expected_output, check_exact=True)