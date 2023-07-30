from sqlalchemy import Integer, String, Float, JSON , DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON 
import jinja2 as j2
import pandas as pd
import numpy as np
import logging
import os
from sqlalchemy.dialects import postgresql
class Load():
    
    @staticmethod
    def load(
        df:pd.DataFrame,
        load_target:str, 
        load_method:str="overwrite",
        target_file_directory:str=None,
        target_file_name:str=None,
        target_database_engine=None,
        target_table_name:str=None
        )->None:
        """
        Load dataframe to either a file or a database.
        Input Parameters:
        - df: pandas dataframe to load.  
        - load_target: choose either `file` or `database`.
        """

        if load_target.lower() == "file": 
            if load_method.lower() == "overwrite": 
                df.to_parquet(f"{target_file_directory}/{target_file_name}")
            elif load_method.lower() == "upsert": 
                if target_file_name in os.listdir(f"{target_file_directory}/"): 
                    df_current = pd.read_parquet(f"{target_file_directory}/{target_file_name}")
                    df_concat = pd.concat(objs=[df_current,df[~df.index.isin(df_current.index)]]) # ~: converts true to false, and false to true. 
                else:
                    pass

        elif load_target.lower() == "database": 
            from sqlalchemy import Table, Column, Integer, String, MetaData, Float
            from sqlalchemy.dialects import postgresql
            if load_method.lower() == "overwrite": 
                df.to_sql(target_table_name, target_database_engine)
            elif load_method.lower() == "upsert":
                stock_price_table = Table(
                target_table_name, meta, 
                Column("stock_code", String, primary_key=True),
                Column("max_open_value_per_day", Float),
                Column("min_open_value_per_day", Float),
                Column("max_close_value_per_day", Float),
                Column("min_close_value_per_day", Float),
                Column("max_high_per_day", Float),
                Column("min_high_per_day", Float),
                Column("max_low_per_day", Float),
                Column("min_low_per_day", Float),
                Column("status_difference", Float),
                Column("trades_mean", Float),
                Column("volume_mean", Float),
            )

                meta.create_all(target_database_engine) # creates table if it does not exist 
                insert_statement = postgresql.insert(stock_price_table).values(df.to_dict(orient='records'))
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['stock_code'],
                    set_={c.key: c for c in insert_statement.excluded if c.key not in ['stock_code']})
                target_database_engine.execute(upsert_statement)
        
        else: 
            raise Exception("Wrong usage of the function. The current config results in no action being performed.")