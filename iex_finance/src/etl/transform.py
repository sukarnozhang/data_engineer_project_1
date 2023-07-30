import pandas as pd
from graphlib import TopologicalSorter
import os
import logging
import jinja2 as j2
class Transform():
    
    @staticmethod
    def transform(
            df:pd.DataFrame
        )->pd.DataFrame:
        """
        Transform the raw dataframes.
        Input Parameters: 
        - df: the dataframe produced from extract_stocks(). 
        """
        # renaming columns
        df = df.rename(columns={
            "numberOfTrades": "numberoftrades"
            })

        #Adding column datetime that includes date and time both. Conversion of minutes into 24-hours format.
        df["datetime"] = pd.to_datetime(df["minute"])
        #Droping columns "date", "minutes" and labels, added datetime in prior step.
        df = df.drop(df.columns[[0,1,2]],axis=1)
        #Difference between open and close value across rows.
        df["difference"] = (df["close"]-df["open"])
        #Creation of new dataframe to analyse the raw dataframe for per day stock vlues.
        df1 = pd.DataFrame()
        # max()  and min() of  OPEN , CLOSE, HIGH, LOW value on per stock  and applying LAMBDA function.
        df1["max_open_value_per_day"] = df.groupby('stock_code').apply(lambda df: df["open"].max())
        df1["min_open_value_per_day"] = df.groupby('stock_code').apply(lambda df: df["open"].min())
        df1["max_close_value_per_day"] = df.groupby('stock_code').apply(lambda df: df["close"].max())
        df1["min_close_value_per_day"] = df.groupby('stock_code').apply(lambda df: df["close"].min())
        df1["max_high_per_day"] = df.groupby('stock_code').apply(lambda df: df["high"].max())
        df1["min_high_per_day"] = df.groupby("stock_code").apply(lambda df: df["high"].min())
        df1["max_low_per_day"] = df.groupby('stock_code').apply(lambda df: df["low"].max())
        df1["min_low_per_day"] = df.groupby("stock_code").apply(lambda df: df["low"].min())
        # Difference sum for per stock
        df1["status_difference"] = df.groupby("stock_code").apply(lambda df: df["difference"].sum())

        #mean() value for trades and volume on per stock by grouby() function and lambda mapping.
        df1["trades_mean"] = df.groupby("stock_code").apply(lambda df: df["numberoftrades"].mean())
        df1["volume_mean"] = df.groupby("stock_code").apply(lambda df: df["volume"].mean())
        
        return df1.reset_index()
    @staticmethod
    def transform_staging(
        model,
        engine, 
        models_path
        )->pd.DataFrame:
        """Transformation applied dataframes from transform() method dataframes.
        Builds models with a matching file name in the models_path folder.
        Input Parameters:
        - model: the name of the model (without .sql)
        - engine: sqlalchemy engine
        - models_path: the path to the models directory containing the sql files. defaults to `models`
        """
        if f"{model}.sql" in os.listdir(models_path):
            # read sql contents into a variable 
            with open(f"{models_path}/{model}.sql") as f: 
                raw_sql = f.read()

            # parse sql using jinja 
            parsed_sql = j2.Template(raw_sql).render(target_table = model, engine=engine)

            # execute parsed sql 
            result = engine.execute(parsed_sql)
            logging.info(f"Successfully built model: {model}, rows inserted/updated: {result.rowcount}")
            return result
        else: 
            return False
            
    #transform_staging(model="staging_stock", engine=engine, models_path="models/")
            