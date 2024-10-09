import pandas as pd
import requests
import asyncio
import websockets
from datetime import datetime
from loguru import logger
from app.data.schemas import KlineColumns
from app.data.exceptions import BinanceAPIError

class BinanceKlines:
    def __init__(self, symbol, interval, start_time, end_time):
        self.symbol = symbol
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time
        self.data = None
        self.websocket_data = pd.DataFrame(columns=KlineColumns.COLUMNS)
        logger.info(f"BinanceKlines initialized with symbol={symbol}, interval={interval}, start_time={start_time}, end_time={end_time}")

    def fetch_and_wrangle_klines(self):
        # Fetch historical data from Binance API
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

    async def websocket_fetch(self, websocket_url):
        logger.info(f"Connecting to WebSocket: {websocket_url}")
        async with websockets.connect(websocket_url) as websocket:
            while True:
                data = await websocket.recv()
                self.process_websocket_data(data)

    def process_websocket_data(self, data):
        # Convert WebSocket data into a DataFrame
        # Assume the data is coming in a format similar to the historical klines
        new_data = pd.DataFrame([data], columns=KlineColumns.COLUMNS)
        
        # Convert necessary columns to correct types
        new_data["open_time"] = pd.to_datetime(new_data["open_time"], unit='ms')
        new_data["close_time"] = pd.to_datetime(new_data["close_time"], unit='ms')
        new_data["open_price"] = new_data["open_price"].astype(float)
        new_data["high_price"] = new_data["high_price"].astype(float)
        new_data["low_price"] = new_data["low_price"].astype(float)
        new_data["close_price"] = new_data["close_price"].astype(float)
        new_data["volume"] = new_data["volume"].astype(float)
        new_data["quote_asset_volume"] = new_data["quote_asset_volume"].astype(float)
        new_data["number_of_trades"] = new_data["number_of_trades"].astype(int)
        new_data["taker_buy_base_asset_volume"] = new_data["taker_buy_base_asset_volume"].astype(float)
        new_data["taker_buy_quote_asset_volume"] = new_data["taker_buy_quote_asset_volume"].astype(float)

        # Append to websocket_data DataFrame
        self.websocket_data = pd.concat([self.websocket_data, new_data], ignore_index=True)

    def combine_historical_and_websocket_data(self):
        logger.info("Combining historical and WebSocket data.")
        combined_df = pd.concat([self.data, self.websocket_data], ignore_index=True)

        # Ensure there are no duplicate timestamps (keep the latest entry)
        combined_df.drop_duplicates(subset=["open_time"], keep="last", inplace=True)
        
        # Handle the index conflict between historical and websocket data
        combined_df.reset_index(drop=True, inplace=True)

        logger.info("Combined data with historical and real-time updates.")
        return combined_df

# Example usage:
symbol = "BTCUSDT"
interval = "1m"
start_time = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)  # 1 day ago in ms
end_time = int(datetime.now().timestamp() * 1000)  # Current time in ms

# Initialize the class
binance_klines = BinanceKlines(symbol, interval, start_time, end_time)

# Fetch historical data
historical_data = binance_klines.fetch_and_wrangle_klines()

# WebSocket URL for real-time data
websocket_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"

# Run WebSocket and combine historical with real-time data
async def main():
    # Start websocket in the background
    await asyncio.gather(
        binance_klines.websocket_fetch(websocket_url),
    )

# Combine historical and real-time data
combined_data = binance_klines.combine_historical_and_websocket_data()

