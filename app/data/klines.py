from app.data.exceptions import BinanceAPIError
from app.data.schemas import KlineColumns
import pandas as pd
import requests
from loguru import logger

class BinanceKlines:
    def __init__(self, symbol, interval):
        self.symbol = symbol
        self.interval = interval
        self.data = None
        logger.info(f"BinanceKlines initialized with symbol={symbol}, interval={interval}")

    def fetch_and_wrangle_klines(self):
        # Fetch data directly from Binance API
        self.data = self.fetch_data_from_binance()
        self.data = self.convert_data_to_dataframe()
        return self.data

    def fetch_data_from_binance(self):
        base_url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": self.symbol,
            "interval": self.interval.lower(),
            "limit": 1400  # Set the limit to the maximum of 1000
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            klines = response.json()

            if not klines:
                raise BinanceAPIError("No klines data returned from Binance API.")

            logger.info(f"Fetched {len(klines)} klines from Binance API.")
            return klines

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from Binance API: {str(e)}")
            raise BinanceAPIError(f"Error fetching data from Binance API: {str(e)}")

    def convert_data_to_dataframe(self):
        logger.info("Converting fetched data to DataFrame.")
        self.data = pd.DataFrame(self.data, columns=KlineColumns.COLUMNS)
        self.data["open_price"] = self.data["open_price"].astype(float)
        self.data["high_price"] = self.data["high_price"].astype(float)
        self.data["low_price"] = self.data["low_price"].astype(float)
        self.data["close_price"] = self.data["close_price"].astype(float)
        self.data["volume"] = self.data["volume"].astype(float)
        self.data["quote_asset_volume"] = self.data["quote_asset_volume"].astype(float)
        self.data["number_of_trades"] = self.data["number_of_trades"].astype(int)
        self.data["taker_buy_base_asset_volume"] = self.data["taker_buy_base_asset_volume"].astype(float)
        self.data["taker_buy_quote_asset_volume"] = self.data["taker_buy_quote_asset_volume"].astype(float)
        self.data["open_time"] = pd.to_datetime(self.data["open_time"], unit='ms')
        self.data["close_time"] = pd.to_datetime(self.data["close_time"], unit='ms')
        self.data = self.data.drop(columns=["ignored"], axis=1)
        logger.info("Data conversion to DataFrame completed.")
        return self.data
    
