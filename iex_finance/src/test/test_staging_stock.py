from database.postgres import PostgresDB
import pandas as pd 
import os 
import jinja2 as j2 
from etl.transform import Transform
def test_staging_stock():
    # -- assemble -- 
    actual_source_staging_stock = "staging_stock"
    mock_source_staging_stock = f"mock_{actual_source_staging_stock}"
    actual_target_table="staging_stock"
    mock_target_table= f"mock_{actual_target_table}"
    path_model_transform = "models/transform"
    engine = PostgresDB.create_pg_engine()
            
    # create mock staging_orders
    df_mock_data_staging_stock = pd.DataFrame({
        "stock_code":["AAPL","TSLA"],
        "status_difference":[-1.1100000000000136,9.055000000000291],
        "trades_mean":[14.592261904761905,20.246290801186944],
        "volume_mean":[2095.535714285714,2442.1364985163204]
    })
    df_mock_data_staging_stock.to_sql(name=mock_source_staging_stock, con=engine, if_exists="replace", index=False)


    # get transform sql file 
    with open(f"{path_model_transform}/{actual_target_table}.sql") as f: 
        raw_sql = f.read()
        
    raw_sql = raw_sql.replace(actual_source_staging_stock, mock_source_staging_stock)
    # write mock transform file 
    with open(f"{path_model_transform}/{mock_target_table}.sql", mode="w") as f:  
            f.write(raw_sql)
    df_expected_output=pd.DataFrame({
        "stock_code":["AAPL","TSLA"],
        "status_difference":[-1.1100000000000136,9.055000000000291],
        "trades_mean":[14.592261904761905,20.246290801186944],
        "volume_mean":[2095.535714285714,2442.1364985163204]  
    })


    # -- act -- 
    staging_stocks= Transform.transform_staging(mock_target_table, engine=engine, models_path=path_model_transform)
    df_output = pd.read_sql(mock_target_table,con=engine)
    # clean up first 
    engine.execute(f"drop table {mock_source_staging_stock}")


    # -- assert -- 
    pd.testing.assert_frame_equal(left=df_output, right=df_expected_output, check_exact=True)