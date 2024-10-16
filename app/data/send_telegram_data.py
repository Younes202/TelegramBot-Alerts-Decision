from loguru import logger
from dotenv import load_dotenv
import os
import requests

load_dotenv()
# Access environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
logger.info(f"BOT_TOKEN: {BOT_TOKEN}, CHANNEL_ID: {CHANNEL_ID}")

def send_telegram_message(TEST_MESSAGE):
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
