# Intraday Stock
A ETL project that retrieves incremental intraday stock data from IEXCloud API on minute basis, aimed to transform it into per day analysis data for per stock.

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
- config_stock_code.yaml - Contains the list of stock symbols to fetch the intraday stocks data

- config.bat - Configure this file with iex_api_key, db_user, db_password, db_server_name, db_database_name if you want to run the project locally in Windows

- config.sh - Configure this file with iex_api_key, db_user, db_password, db_server_name, db_database_name if you want to run the project locally in Linux/Mac

- .env file - This file is required to build and run the docker image on Cloud

## Usage
The pipeline will retrieve intraday stock data for the specified stock symbol, apply transformations, and load the transformed data into the PostgreSQL database.

## Extract
Real world APIs are utilised in extracting data. The data contains per minute stock analysis for different stock codes. The data can be accessed from passing parameters as to get requests from API

params = {
            "token" : iex_api_key
        }
where, iex_api_key is the secret key.

base_url = f"https://cloud.iexapis.com/stable/stock/{stock_ticker}/intraday-prices"

where iex_api_key is the secret key.

In the base url, stock ticker is taken from config_stock_code.yaml file, which is user configurable. Stock code can be changed according to user's preferences.

The extraction part takes out table with columns as json file:

```json
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
        "changeOverTime": -0.0039  
    }
```

The above "x" doesn't contain stock_code, so in the final extracted data "stock_code" for each "x" is appended. So, each row contains column "stock_code" as extra element.


## Transformations
Transformations are made with the aim to convert per minute data to per day data for per stock.

Sample Output of the transformation :
<img width="1106" src="https://github.com/sukarnozhang/data_engineer_project_1/blob/main/iex_finance/src/data/sample_output.png">

## Load
When loading the database into pgAdmin, tables need to be loaded. If a table is being loaded for the first time, a new table is created in pgAdmin. If new data comes from the website, the upsert function is used.

## ETL Pipeline
To run the pipeline in your local system, execute the following command:
```sh
cd <src folder in local system>

export PYTHONPATH=`pwd`

python etl/pipeline/pipeline.py
```
The ETL pipeline begins with metadata logging initialization using logging.info to track its history, as implemented in the utility file metadata_logging.py. Logs are recorded in the table specified by log_table: pipeline_logs.


## Testing
Unit testing is applied to the transformed data in the stocks_intraday table to create weekly aggregated data.


## Docker file
The Dockerfile is initialized outside the src directory. The environment variables for the Dockerfile are defined in the .env file, which can be configured according to the user’s credentials.


## AWS Configuration
- Create a private S3 bucket to store the .env file, which will hold runtime variables and secrets.
- Create a PostgreSQL database in the cloud using the AWS RDS service.
- Create an appropriate inline IAM policy required by the ECS service.
- Create an appropriate IAM policy for users to run ECS tasks.
- Connect the Dockerfile with ECS credentials and update the credentials to use the RDS server endpoint.
- Run the Dockerfile on ECS and maintain logs of the execution.
- For cron scheduling, an interval of ten minutes is configured. Since the API is updated on a minute-by-minute basis, a preferred interval of twenty minutes is recommended.


