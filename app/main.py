import uvicorn
from fastapi import FastAPI
from loguru import logger

from app.data.klines import BinanceKlines  # Assuming this is your custom BinanceKlines module
from app.strategies.indicators import get_opportunity
from pydantic import BaseModel
import asyncio
import requests

from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Access environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")


app = FastAPI()

# Define ResultOrder for notification
class ResultOrder(BaseModel):
    symbol: str
    close_time: str
    close_price: float
    opportunity: str
    message: str

# Utility function to fetch data for a symbol and check for an opportunity
async def fetch_and_check_opportunity(symbol: str):
    try:
        interval = "1m"
        logger.info(f"Fetching data for {symbol}...")

        # Fetching kline data for the symbol
        klines_instance = BinanceKlines(symbol, interval)
        data = klines_instance.fetch_and_wrangle_klines()

        # Apply strategy and get trading opportunity
        close_time, close_price, opportunity = get_opportunity(data)

        if opportunity:
            logger.info(f"Opportunity of {opportunity} for {symbol} at close price {close_price}")

            return ResultOrder(
                symbol=symbol,
                close_time=close_time,
                close_price=close_price,
                opportunity=opportunity,
                message=f"Opportunity found for {symbol}: {opportunity} at close price {close_price}"
            )
        else:
            logger.info(f"No Opportunity for {symbol} at close price {close_price}")
            return None

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return None

# Function to send a message to the Telegram channel
def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHANNEL_ID,  # Use the channel ID instead of group chat ID
        'text': message,
        'parse_mode': 'Markdown'  # Optional: enables markdown for better formatting
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            logger.info("Message sent successfully")
        else:
            logger.error(f"Failed to send message: {response.status_code}, {response.text}")
    except Exception as e:
        logger.error(f"Error sending message: {str(e)}")

# Function to fetch opportunities for multiple symbols and send messages
async def fetch_opportunities_for_symbols(symbols):
    while True:
        opportunities_dict = {}

        # Fetch data and check opportunity for all symbols concurrently
        tasks = [fetch_and_check_opportunity(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)

        # Populate the dictionary with opportunities
        for result in results:
            if result:
                opportunities_dict[result.symbol] = {
                    "opportunity": result.opportunity,
                    "close_time": result.close_time,
                    "close_price": result.close_price
                }

        # Check if the dictionary is empty and send messages accordingly
        if opportunities_dict:
            print(opportunities_dict)  # Print the dictionary if not empty
            logger.info(f"Opportunities: {opportunities_dict}")

            # Send a message to the channel for each opportunity found
            for symbol, opportunity_data in opportunities_dict.items():
                message = (
                    f"🚀 *Trading Opportunity {{symbol}} Alert!*\n\n"
                    f"🔹 **Type:** *{opportunity_data['opportunity']}*\n"
                    f"💰 **Price:** `{opportunity_data['close_price']:.2f}` USDT\n"
                    f"⏰ **Time:** `{opportunity_data['close_time']}`\n\n"
                    f"🔔 Stay tuned for more opportunities!"
                )
                send_telegram_message(message)

        else:
            logger.info("No opportunities found. Dictionary is empty.")

        # Sleep for 1 minute before checking again
        await asyncio.sleep(60)

# FastAPI startup event to start monitoring
@app.on_event("startup")
async def startup_event():
    symbols = ["BTCUSDT", "BNBUSDT", "ETHUSDT", "DOTUSDT", "PAXGUSDT"]  # You can modify symbols as needed
    logger.info("Starting monitoring for symbols...")

    # Start fetching opportunities
    await fetch_opportunities_for_symbols(symbols)

if __name__ == "__main__":
    # Start the FastAPI server
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
