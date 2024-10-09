from enum import Enum
class KlineColumns:
    """Defines the columns for the Binance Klines DataFrame."""
    COLUMNS = [
        "open_time", "open_price", "high_price", "low_price", "close_price", "volume", "close_time",
        "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume", "ignored"
    ]

# Define an Enum for Valid Binance Trading Pairs with USDT
class CryptoPair(Enum):
    BTC_USDT = "BTCUSDT"
    ETH_USDT = "ETHUSDT"
    BNB_USDT = "BNBUSDT"
    ADA_USDT = "ADAUSDT"
    SOL_USDT = "SOLUSDT"
    DOT_USDT = "DOTUSDT"
    XRP_USDT = "XRPUSDT"
    DOGE_USDT = "DOGEUSDT"
    AVAX_USDT = "AVAXUSDT"
    LTC_USDT = "LTCUSDT"
    MATIC_USDT = "MATICUSDT"
    LINK_USDT = "LINKUSDT"
    UNI_USDT = "UNIUSDT"


