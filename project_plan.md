# Project plan
## Objective
Stock Market Investing is a tough job for retail investors. You can either rely on Fund Managers or do your own analysis to find the right stocks.
With this project we want to provide insights to investors to make better investing/trading decisions.
## Consumers
What users would find your data useful? How do they want to access the data?
Retail Investors and Data analysts are able to pull the data from AWS Services like RDS and S3.
## Questions
What questions are you trying to answer with your data? How will your data support your users?

Most stable stocks from each sector for each day. This would help retail investors in planning their intraday shortterm trades.
Best stocks for Intraday swing trading, based on most variance/swing. This would help retail investors in planning their intraday and swing trades.
Upward momentum stocks for monthly Calls. This would help retail investors in Option trading for Calls.
Downward momentum for daily Puts. This would help retail investors in Option trading for Puts.
## Source datasets
What datasets are you sourcing from? How frequently are the source datasets updating?
The source of dataset is sourced from IEX Cloud Legacy API.
IEX Cloud provides real time prices, volume, and quotes for all NMS US securities
Methods:
GET /stock/{symbol}/intraday-prices
Datasets are updating every minute. The data set is delayed by 15 minutes for free accounts.
## Solution architecture
How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution? How do we automate the entire running of the solution?
Process Flow
## Usage
The pipeline will retrieve intraday stock data for the specified stock symbol, apply transformations, and load the transformed data into the PostgreSQL database.

## Extract
(path=src/etl/extract.py)
Real world API's are utilised in extrcating data. The data contains per minute stock analysis from different stock code.The data can be accessed from
passing parameters as to get requests from API

params = {
            "token" : iex_api_key
        }
where, iex_api_key is the secret key.

base_url = f"https://cloud.iexapis.com/stable/stock/{stock_ticker}/intraday-prices"

In the base url, stock ticker is taken config_stock_code.yaml file , which is user configurable.Stock code can changed according to user's preferences.

The extraction part takes out table with columns as json file:
    x = {
        "date": "2017-12-15",
        "minute": "09:30",
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
(path = src/etl/transform.py)
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

## Incremental Staging on transformations
Applying incremental staging on transformed data to take out columns only that is required in everyday trading by taking four important columns from "stocks_intraday"(transformed in prior step) through jinja file path as "src\models\transform\stocks_intraday.sql". Applying pivot point as max value operation "volume_mean".

## Load
(path=src/etl/load.py)
In loading the database to the pgadmin,tables need to be loaded. If tables are first time loaded then new table is inserted in the pgadmin. If new data from website comes in "Upsert" function is utilised. 
For staging transformational loading(staging_stock) , it is sourced from newly upserted transformed table(stocks_intraday) loaded in pgadmin.
LOAD
Use SQLAlchemy to connect with Postgres database in AWS and upload the final datasets.
Use Python BOTO Library to connect and upload the inital and final datasets to S3.
Solution Architecture
Use Github to host project code and documentation.
Build the local project as a docker file.
Host the project as a Docker file in ECR.
Create private S3 Bucket to place the .env file which will store runtime variables and secrets.
Create S3 bucket to store the inital and final datasets to S3
Create Postgres Database in Cloud using AWS RDS Web Service.
Create appropriate inline IAM policy that would be required by the ECS service.
Create appropriate IAM policy for the users to run the ECS tasks.

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
## Breakdown of tasks
How is your project broken down? Who is doing what?

-Create Github Repository - Ajay
-Create Draft Project Plan - Sukarno, Puja, Ajay
-Extract - Puja
-Transform - Perform transformations using Pandas DF APIs - Puja
-Pipeline - Ajay
-Load - Sukarno
-Logging - Puja
-Testing - Puja
-Generating the Docker File - Sukarno, Puja, Ajay
-Create private S3 Bucket to place the .env file - Sukarno
-Create S3 bucket to store the inital and final datasets to S3 - Sukarno,Puja, Ajay
-Create Postgres Database in Cloud using AWS RDS Web Service - Sukarno
-Create appropriate inline IAM policy that would be required by the ECS service - Sukarno, Puja, Ajay
-Create appropriate IAM policy for the users to run the ECS tasks - Sukarno, Puja, Ajay
-ECS - Sukarno, Puja, Ajay
-Documentation - Sukarno, Puja, Ajay
