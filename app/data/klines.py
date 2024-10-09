from app.data.exceptions import BinanceAPIError
from app.data.schemas import KlineColumns
import pandas as pd
import requests
from loguru import logger

class BinanceKlines:
    def __init__(self, symbol, interval, start_time, end_time):
        self.symbol = symbol
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.data = None
        logger.info(f"BinanceKlines initialized with symbol={symbol}, interval={interval}, start_time={start_time}, end_time={end_time}")

    def fetch_and_wrangle_klines(self):
        # Fetch data directly from Binance API
        self.data = self.fetch_data_from_binance()
        self.data = self.convert_data_to_dataframe()
        return self.data

    def fetch_data_from_binance(self):
        base_url = "https://api.binance.com/api/v3/klines"
        all_klines = []
        current_start_time = self.start_time

        while current_start_time < self.end_time:
            params = {
                "symbol": self.symbol,
                "interval": self.interval.lower(),
                "startTime": current_start_time,
                "endTime": self.end_time,
                "limit": 1000  # Set the limit to the maximum of 1000
            }

            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                klines = response.json()

                if not klines:
                    break  # Break the loop if no more data is returned

                all_klines.extend(klines)
                logger.info(f"Fetched {len(klines)} klines from Binance API.")

                # Update current_start_time to the next batch
                current_start_time = klines[-1][0] + 1  # Add 1 ms to avoid overlap

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from Binance API: {str(e)}")
                raise BinanceAPIError(f"Error fetching data from Binance API: {str(e)}")
        
        return all_klines

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
