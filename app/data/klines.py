import os
from dotenv import load_dotenv
from app.data.exceptions import BinanceAPIError
from app.data.schemas import KlineColumns
import pandas as pd
import requests
from loguru import logger

# Load environment variables
load_dotenv(".env")

class BinanceKlines:
    def __init__(self, symbol, interval):
        self.symbol = symbol
        self.interval = interval
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.data = None
        logger.info(f"Initialized: {symbol}, {interval}")

    def fetch_and_wrangle_klines(self):
        logger.info(f"Fetching klines: {self.symbol}, {self.interval}")
        self.data = self.fetch_data_from_binance()
        return self.convert_data_to_dataframe() if self.data else None

    def fetch_data_from_binance(self):
        base_url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": self.symbol, "interval": self.interval.lower(), "limit": 1000}
        headers = {"X-MBX-APIKEY": self.api_key}

        try:
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            klines = response.json()
            if not klines:
                raise BinanceAPIError("No klines data returned")
            return klines
        except requests.exceptions.RequestException as e:
            raise BinanceAPIError(f"Binance API error: {str(e)}")

    def convert_data_to_dataframe(self):
        try:
            df = pd.DataFrame(self.data, columns=KlineColumns.COLUMNS)
            for col in ["open_price", "high_price", "low_price", "close_price", "volume", "quote_asset_volume", 
                        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]:
                df[col] = df[col].astype(float)
            df["number_of_trades"] = df["number_of_trades"].astype(int)
            df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
            df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')
            return df.drop(columns=["ignored"], axis=1)
        except Exception as e:
            raise BinanceAPIError(f"Data conversion error: {str(e)}")
