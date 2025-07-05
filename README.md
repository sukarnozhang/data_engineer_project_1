# Intraday Stock
A ETL project that retrieves incremental intraday stock data from IEXCloud API on minute basis is aimed to transformed to per day analysis data for per stock.

<img width="1106" alt="Screenshot 2024-05-28 at 10 24 17" src="https://github.com/sukarnozhang/data_engineer_project_1/assets/78150905/1886cb28-8a85-4fc6-a504-84af2031aa69">

## Requirements
- jupyter==1.0.0
- pandas==1.4.3
- requests==2.28.1
- SQLAlchemy==1.4.39
- pyarrow==8.0.0
- pg8000==1.29.1
- pytest==7.1.2
- pylint==2.14.4
- pyyaml==6.0
- Jinja2==3.1.2
- schedule==1.1.0

## Installation
```sh
pip install -r requirements.txt
```
## Configuration
- config_stock_code.yaml - Contains the list of Stock symbols to fetch the intraday stocks data
- config.bat - Configure this file with iex_api_key, db_user, db_password, db_server_name, db_database_name if you want to run the project locally in Windows
- config.sh - Configure this file with iex_api_key, db_user, db_password, db_server_name, db_database_name if you want to run the project locally in Linux/Mac
- .env file - This file is required to build and run the docker image on Cloud

## Usage
The pipeline will retrieve intraday stock data for the specified stock symbol, apply transformations, and load the transformed data into the PostgreSQL database.

## Extract
(path=src/etl/extract.py)
Real world API's are utilised in extracting data. The data contains per minute stock analysis for different stock code.The data can be accessed from
passing parameters as to get requests from API

params = {
            "token" : iex_api_key
        }
where, iex_api_key is the secret key.

base_url = f"https://cloud.iexapis.com/stable/stock/{stock_ticker}/intraday-prices"

In the base url, stock ticker is taken config_stock_code.yaml file , which is user configurable.Stock code can changed according to user's preferences.

The extraction part takes out table with columns as json file:
    x = {
        "date": "2023-12-15",
        "minute": "2023-12-15 09:30AM",
        "label": "09:30 AM",
        "marketOpen": 143.98,
        "marketClose": 143.775,
        "marketHigh": 143.98,
        "marketLow": 143.775,
        "marketAverage": 143.889,
        "marketVolume": 3070,
        "marketNotional": 441740.275,
        "marketNumberOfTrades": 20,
        "marketChangeOverTime": -0.004,
        "high": 143.98,
        "low": 143.775,
        "open": 143.98,
        "close": 143.775,
        "average": 143.889,
        "volume": 3070,
        "notional": 441740.275,
        "numberOfTrades": 20,
        "changeOverTime": -0.0039,
    }

The above "x" doesn't contains stock_code, so in the final extrcated data "stock_code" for each "x" is appended.So,the each row contains columns "stock_code" as extra element.

## Transformations
(path = src/etl/transform.py)<br><br>
Transformations are made in aim to convert per minute data to per day data for per stock.
The following transformations are applied to the data to transformed table as stocks_intraday:
    #Renaming
        df = df.rename(columns={
            "numberOfTrades": "numberoftrades"
            })

    #Adding column datetime that includes date and time both. Conversion of minutes into 24-hours format.
        df["datetime"] = pd.to_datetime(df["minute"])

    #Droping columns "date", "minutes" and labels, added datetime in prior step.
        df = df.drop(df.columns[[0,1,2]],axis=1)

    #Difference between open and close value across rows.
        df["difference"] = (df["close"]-df["open"])

    # Initialize summary DataFrame
        summary_df = pd.DataFrame()

    # Compute daily max/min open, close, high, low values grouped by stock_code
        grouped = df.groupby("stock_code")
        summary_df["max_open_value_per_day"] = grouped["open"].max()
        summary_df["min_open_value_per_day"] = grouped["open"].min()
        summary_df["max_close_value_per_day"] = grouped["close"].max()
        summary_df["min_close_value_per_day"] = grouped["close"].min()
        summary_df["max_high_per_day"] = grouped["high"].max()
        summary_df["min_high_per_day"] = grouped["high"].min()
        summary_df["max_low_per_day"] = grouped["low"].max()
        summary_df["min_low_per_day"] = grouped["low"].min()

    # Difference sum for per stock
        summary_df["status_difference"] = grouped["difference"].sum()

    #mean() value for trades and volume on per stock by grouby() function and lambda mapping.
        summary_df["trades_mean"] = grouped["numberOfTrades"].mean()
        summary_df["volume_mean"] = grouped["volume"].mean()

Sample Output of the transformation :


## Load
(path=src/etl/load.py)
In loading the database to the pgadmin,tables need to be loaded. If tables are first time loaded then new table is inserted in the pgadmin. If new data from website comes in "Upsert" function is utilised. 
For staging transformational loading(staging_stock) , it is sourced from newly upserted transformed table(stocks_intraday) loaded in pgadmin.

## ETL Pipeline
To run the pipeline in your local system, execute the following command:
```sh
cd <src folder in local system>

export PYTHONPATH=`pwd` # for windows: set PYTHONPATH=%cd%

python etl/pipeline/pipeline.py
```
ETL pipeline is executed from metadata logging initialisation and logging.info for keeping the history as coded in utility folder as "metadata_logging.py" and ectracting the log through initialising the "log_table: "pipeline_logs" on config.yaml file.
Extract, Transform and Load are performed to get final loaded on pgadmin as "stocks_intraday" , "staging_stocks" and "pipeline_logs".

## Testing
(jinja file path="src\models\transform"),(test file path = "src\test\transform")
Two unit testings are applied on the transformational datas on table stocks_intraday and staging_stock through pytest command after running the pipeline.jinja file to structure the mock table.
## Docker file
(path="\iex_finance\Dockerfile")
(env file path = "\iex_finance\.env")
The docker file is initialised outside the src directory. The environment variables foe docker file are initilaised on .env file which is configurable according to users credentials.
## Code:
Login in your docker hub in terminal before executing these commands.

Build the docker image using the following command
```sh
docker build . -t dockerhub_username/<name_of_your_tag>
```
Run the docker image
```sh
docker run --env-file .env <image_name>
```
docker push image
## PostgreSQL Database
In the sql query , go the desired database as in configuration. 

Code:
Select * from <table_name>;

Table name can be stocks_intraday, staging_stocks, pipeline_logs
## AWS Configuration
-Create private S3 Bucket to place the .env file which will store runtime variables and secrets.
-Create Postgres Database in Cloud using AWS RDS Web Service.
-Create appropriate inline IAM policy that would be required by the ECS service.
-Create appropriate IAM policy for the users to run the ECS tasks.
-Connect Docker file with ECS credentials ##change the credentials to Endpoint of RDS server.
-Run the docker file on the ECS and keep logs of running.
-For Cron scheduling, the time period of ten minutes is taken.Since,the API is updated minute to minute basis, so interval of twenty minutes is preferred.


