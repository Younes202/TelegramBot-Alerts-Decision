import os
from dotenv import load_dotenv
from app.data.exceptions import BinanceAPIError
from app.data.schemas import KlineColumns
import pandas as pd
import requests
from loguru import logger

# Load environment variables from .env file
load_dotenv()

class BinanceKlines:
    def __init__(self, symbol, interval):
        """
        Initialize the BinanceKlines instance with a symbol and interval.
        
        Parameters
        ----------
        symbol : str
            The trading symbol, e.g., 'BTCUSDT'.
        interval : str
            The time interval for fetching klines data, e.g., '1m'.
        """
        self.symbol = symbol
        self.interval = interval
        self.api_key = os.getenv("BINANCE_API_KEY")  # Load API key from .env file
        self.data = None
        logger.info(f"BinanceKlines initialized with symbol={symbol}, interval={interval}")

    def fetch_and_wrangle_klines(self):
        """
        Fetches and processes kline data from the Binance API.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the processed kline data.
        """
        logger.info(f"Starting to fetch and wrangle klines for {self.symbol} with interval {self.interval}.")
        self.data = self.fetch_data_from_binance()
        if self.data is not None:
            logger.info(f"Data fetched successfully for {self.symbol}. Converting to DataFrame.")
            self.data = self.convert_data_to_dataframe()
            return self.data
        else:
            logger.warning(f"No data returned for {self.symbol}.")
            return None

    def fetch_data_from_binance(self):
        """
        Fetches kline data from the Binance API.

        Returns
        -------
        list
            A list of klines fetched from the API.
        """
        base_url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": self.symbol,
            "interval": self.interval.lower(),
            "limit": 1000  # Set the limit to 1000 for better performance
        }

        headers = {
            "X-MBX-APIKEY": self.api_key  # API key is included in the header for authentication
        }

        try:
            logger.info(f"Requesting Binance API for {self.symbol} with parameters: {params}")
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            klines = response.json()

            if not klines:
                logger.error(f"No kline data returned from Binance API for {self.symbol}.")
                raise BinanceAPIError("No klines data returned from Binance API.")

            logger.info(f"Successfully fetched {len(klines)} klines for {self.symbol}.")
            return klines

        except requests.exceptions.RequestException as e:
            logger.error(f"Request to Binance API failed: {str(e)} for {self.symbol} with interval {self.interval}.")
            raise BinanceAPIError(f"Error fetching data from Binance API: {str(e)}")

    def convert_data_to_dataframe(self):
        """
        Converts the raw klines data into a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the processed kline data.
        """
        try:
            logger.info("Converting fetched data into a pandas DataFrame.")
            df = pd.DataFrame(self.data, columns=KlineColumns.COLUMNS)
            
            # Convert data types for numerical columns
            df["open_price"] = df["open_price"].astype(float)
            df["high_price"] = df["high_price"].astype(float)
            df["low_price"] = df["low_price"].astype(float)
            df["close_price"] = df["close_price"].astype(float)
            df["volume"] = df["volume"].astype(float)
            df["quote_asset_volume"] = df["quote_asset_volume"].astype(float)
            df["number_of_trades"] = df["number_of_trades"].astype(int)
            df["taker_buy_base_asset_volume"] = df["taker_buy_base_asset_volume"].astype(float)
            df["taker_buy_quote_asset_volume"] = df["taker_buy_quote_asset_volume"].astype(float)
            df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
            df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')

            # Remove unnecessary column
            df = df.drop(columns=["ignored"], axis=1)

            logger.info("Data conversion to DataFrame completed successfully.")
            return df

        except Exception as e:
            logger.error(f"Failed to convert klines data into DataFrame: {str(e)}")
            raise BinanceAPIError(f"Error converting data to DataFrame: {str(e)}")
