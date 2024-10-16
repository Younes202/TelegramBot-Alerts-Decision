import pandas_ta as ta
from app.strategies.schemas import DataFrameUtils
from app.strategies.exceptions import StrategyError
import pytz
from datetime import timedelta

# Timezone for Morocco
morocco_tz = pytz.timezone('Africa/Casablanca')

class Strategy:
    def __init__(self, data):
        self.original_data = data.copy()

    def _apply_strategy(self, strategy_function):
        try:
            data_copy = self.original_data.copy()
            data_copy = strategy_function(data_copy)
            data_copy = DataFrameUtils.fill_missing_values(data_copy)
            return data_copy
        except Exception as e:
            raise StrategyError(f"Failed to apply strategy: {e}")

    def rsi_bollinger_buy_strategy(self):
        def strategy_logic(data):
            # Calculate RSI with a default period (14)
            data['RSI'] = ta.rsi(data['close_price'], length=14)

            # Calculate Bollinger Bands (20 period, 2 standard deviation)
            bbands = ta.bbands(data['close_price'], length=20, std=2.0)
            data['BB_lower'] = bbands['BBL_20_2.0']  # Lower Bollinger Band
            data['BB_upper'] = bbands['BBU_20_2.0']  # Upper Bollinger Band

            # Buy signal if RSI < 40 and price is close to the lower Bollinger Band
            data['opportunity_type'] = data.apply(
                lambda row: "Buy" if row['RSI'] < 40 and row['close_price'] <= row['BB_lower'] else None,
                axis=1
            )
            return data

        return self._apply_strategy(strategy_logic)


def get_opportunity(data):
    strategy = Strategy(data)
    result_data = strategy.rsi_bollinger_buy_strategy()

    # Localize close_time to UTC
    result_data['close_time'] = result_data['close_time'].dt.tz_localize('UTC')

    # Convert the close_time to Morocco timezone
    close_time = result_data['close_time'].iloc[-1].astimezone(morocco_tz)

    if close_time.second == 59:
        close_time += timedelta(seconds=1)
        close_time = close_time.replace(second=0)

    close_time_str = close_time.strftime('%Y-%m-%d %H:%M:%S')

    return close_time_str, result_data['close_price'].iloc[-1], result_data['opportunity_type'].iloc[-1]
