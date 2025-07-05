import pandas as pd
import yaml
import requests

class Extract:
    """
    A class to extract intraday stock data from the IEX Cloud API.
    """

    @staticmethod
    def extract_per_stock(iex_api_key: str, stock_ticker: str) -> pd.DataFrame:
        """
        Extract intraday data for a single stock from the IEX Cloud API.

        Parameters:
        - iex_api_key (str): API key for IEX Cloud.
        - stock_ticker (str): Stock ticker symbol (e.g., 'AAPL' for Apple).

        Returns:
        - pd.DataFrame: A flattened DataFrame of the intraday stock price data.
        """

        base_url = f"https://cloud.iexapis.com/stable/stock/{stock_ticker}/intraday-prices"
        params = {"token": iex_api_key}

        # Send GET request to the API
        response = requests.get(base_url, params=params)

        # Check response status
        if response.status_code == 200:
            stock_data = response.json()
        else:
            raise Exception(
                f"Failed to extract data for {stock_ticker}. "
                f"Status code: {response.status_code}. "
                "Check API limits or stock ticker validity."
            )

        # Convert JSON data to a flattened pandas DataFrame
        df_stock = pd.json_normalize(stock_data)

        return df_stock

    @staticmethod
    def extract_stocks(iex_api_key: str) -> pd.DataFrame:
        """
        Extract intraday data for multiple stocks listed in a YAML config file.

        Parameters:
        - iex_api_key (str): API key for IEX Cloud.

        Returns:
        - pd.DataFrame: A concatenated DataFrame with intraday data for all stocks.
        """

        # Load the list of stock codes from the YAML config file
        with open("config_stock_code.yaml") as file:
            config = yaml.safe_load(file) # to avoid executing script within yaml

        # Get stock codes list; fallback to empty list if missing
        stock_codes = config.get("stock_code", [])
        if not stock_codes:
            raise ValueError("No stock codes found in config_stock_code.yaml.")

        # Initialize empty DataFrame to hold combined data
        df_all = pd.DataFrame()

        # Loop through each stock code and extract its data
        for stock in stock_codes:
            # Extract intraday data for this stock
            df_stock = Extract.extract_per_stock(iex_api_key=iex_api_key, stock_ticker=stock)
            
            # Add a column to indicate which stock the data belongs to
            df_stock["stock_code"] = stock
            
            # Concatenate with the master DataFrame
            # ignore_index ensures the combined DataFrame has sequential integer index
            df_all = pd.concat([df_all, df_stock], ignore_index=True)

        return df_all
