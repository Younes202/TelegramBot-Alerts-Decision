
import uvicorn
from fastapi import FastAPI
from app.data.klines import BinanceKlines
from app.strategies.indicators import get_opportunity
from pydantic import BaseModel
import asyncio
from loguru import logger
from dotenv import load_dotenv
import os
import requests

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
# Access environment variables
BOT_TOKEN = '7151560661:AAGQDuwxfXxNwgFeq6n-A5aKzx7xd5l4wNw'
CHANNEL_ID = '-1002318535059'
logger.info(f"BOT_TOKEN: {BOT_TOKEN}, CHANNEL_ID: {CHANNEL_ID}")

async def send_telegram_message(TEST_MESSAGE):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHANNEL_ID,
        'text': TEST_MESSAGE
    }
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        logger.info("Message sent successfully!")
    else:
        logger.error(f"Failed to send message: {response.status_code}, {response.text}")

app = FastAPI()

# Define ResultOrder for notification
class ResultOrder(BaseModel):
    symbol: str
    close_time: str
    close_price: float
    opportunity: str
    message: str

# This list will track all buy/sell signals
signals = {}


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
        logger.info(f"Opportunity detected: {opportunity} for {symbol}.")

        # Buy Opportunity
        if opportunity == "Buy" and symbol not in signals:
            signals[symbol] = {
                "buy_price": close_price,
                "buy_time": close_time
            }

            # Send buy opportunity message to Telegram
            message = (
                f"🚀 Buy Opportunity for {symbol}!\n"
                f"💰 Price: {signals[symbol]['buy_price']}\n"
                f"⏰ Time: {signals[symbol]['buy_time']}\n"
                f"🔔 Stay tuned for more opportunities!"
            )
            await send_telegram_message(message)

        # Sell Opportunity based on risk management
        elif symbol in signals:
            buy_price = signals[symbol]['buy_price']
            profit = close_price - buy_price
            risk_threshold = buy_price * 0.15  # Example: 15% profit threshold

            # Sell condition: Profit greater than or equal to the risk threshold
            if close_price > buy_price and profit >= risk_threshold:
                signals[symbol].update({
                    "sell_price": close_price,
                    "sell_time": close_time,
                    "profit": profit
                })
                logger.info(f"Sell opportunity for {symbol}. Profit: {profit}")

                # Send sell opportunity message to Telegram
                message = (
                    f"🚀 Sell Opportunity for {symbol}!\n"
                    f"💰 Buy Price: {buy_price}\n"
                    f"💰 Sell Price: {close_price}\n"
                    f"💰 Profit: {profit}\n"
                    f"⏰ Buy Time: {signals[symbol]['buy_time']}\n"
                    f"⏰ Sell Time: {close_time}\n"
                    f"🔔 Stay tuned for more opportunities!"
                )
                await send_telegram_message(message)

                # Remove the signal after selling
                del signals[symbol]

            else:
                logger.info(f"No sell opportunity for {symbol} at {close_price}.")

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return None

# Function to fetch opportunities for multiple symbols
async def fetch_opportunities_for_symbols(symbols):
    while True:
        for symbol in symbols:
            await fetch_and_check_opportunity(symbol)

        # Sleep for 1 minute before checking again
        await asyncio.sleep(60)

# FastAPI startup event to start monitoring
@app.on_event("startup")
async def startup_event():
    symbols = [
        "BTCUSDT",  # Bitcoin
        "ETHUSDT",  # Ethereum
        "BNBUSDT",  # Binance Coin
        "SOLUSDT",  # Solana
        "MATICUSDT",  # Polygon
        "XRPUSDT"  # Ripple
    ]

    logger.info("Starting monitoring for symbols...")
    logger.info(f"BOT_TOKEN: {BOT_TOKEN}, CHANNEL_ID: {CHANNEL_ID}")

    # Start fetching opportunities
    await fetch_opportunities_for_symbols(symbols)

# Main entry point to run the FastAPI server
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
