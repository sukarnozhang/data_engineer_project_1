import pandas as pd
import yaml
import requests
class Extract():
    @staticmethod
    def extract_per_stock(
            iex_api_key:str,
            stock_ticker:str=None
        )->pd.DataFrame:
        """
        Method to extract data for a stock from the intraday stock API
        Input Parameters:
        - iex_api_key: api key 
        - stock_ticker: code of the stock e.g. AAPL for APPLE stock
        """       
        params = {
            "token" : iex_api_key
        }
        base_url = f"https://cloud.iexapis.com/stable/stock/{stock_ticker}/intraday-prices"
        response = requests.get(base_url,params=params)
        if response.status_code == 200: 
            stock_data = response.json()
        else: 
            raise Exception("Extracting weather api data failed. Please check if API limits have been reached.")
        df_stock_codes = pd.json_normalize(stock_data)
        return df_stock_codes
    
    @staticmethod
    def extract_stocks(
            iex_api_key:str 
        )->pd.DataFrame:
        """
        Method to extract data for all stocks defined in config_stock_code.yaml
        Input Parameters:
        - iex_api_key: api key
        """  
        # read list of stock_codes
        
        with open("config_stock_code.yaml") as file:
            data = yaml.safe_load(file)
        stock_codes = data["stock_code"]
        df = pd.DataFrame({"Stock Code": stock_codes})
        df_concat = pd.DataFrame()
        for stock in df["Stock Code"]:
            df_extracted = Extract.extract_per_stock(stock_ticker=stock,iex_api_key=iex_api_key)
            df_extracted["stock_code"]=stock
            df_concat = pd.concat([df_concat,df_extracted])
        return df_concat