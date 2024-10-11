import pandas_ta as ta
from app.strategies.schemas import DataFrameUtils
from app.strategies.exceptions import StrategyError
import pytz
from datetime import timedelta

# Assuming opportunity_data['close_time'] is a UTC datetime object
morocco_tz = pytz.timezone('Africa/Casablanca')

class Strategy:
    def __init__(self, data):
        """
        Initialize the Strategy with a dataset.

        Parameters
        ----------
        data : pandas.DataFrame
            The dataset containing price and volume information.
        """
        self.original_data = data.copy()  # Store the original dataset

    def _apply_strategy(self, strategy_function):
        """Helper method to apply a strategy function and fill missing values."""
        try:
            data_copy = self.original_data.copy()  # Work on a copy of the original data
            data_copy = strategy_function(data_copy)
            data_copy = DataFrameUtils.fill_missing_values(data_copy)
            return data_copy
        except Exception as e:
            raise StrategyError(f"Failed to apply strategy: {e}")

    def rsi_bb_volume_strategy(self):
        """
        Hybrid strategy combining RSI, Bollinger Bands, and Volume.
        """
        def rsi_bb_volume(data):
            # Apply RSI
            data['RSI'] = ta.rsi(data['close_price'], length=7)
            # Apply Bollinger Bands
            bands = ta.bbands(data['close_price'], length=20)
            data['BB_lower'] = bands['BBL_20_2.0']
            data['BB_upper'] = bands['BBU_20_2.0']
            # Apply Volume
            data['volume_prev'] = data['volume'].shift(1)

            # Determine opportunities based on a combination of signals
            data['opportunity_type'] = data.apply(
                lambda row: "Buy" if row['RSI'] < 30 and row['close_price'] <= row['BB_lower'] and row['volume'] > row['volume_prev']
                else "Sell" if row['RSI'] > 70 and row['close_price'] >= row['BB_upper'] and row['volume'] > row['volume_prev']
                else None,
                axis=1
            )
            return data

        return self._apply_strategy(rsi_bb_volume)


def get_opportunity(data):
    """
    Applies a strategy to the data and returns the close price, close time, and opportunity type.

    Parameters
    ----------
    data : pandas.DataFrame
        The dataset containing price and volume information.

    Returns
    -------
    tuple
        A tuple with close_time, close_price, and opportunity_type for the last entry.
    """
    # Initialize the strategy with the given data
    strategy = Strategy(data)

    # Apply the hybrid strategy to the data
    result_data = strategy.rsi_bb_volume_strategy()

    # Localize close_time to UTC (tz-naive timestamps)
    result_data['close_time'] = result_data['close_time'].dt.tz_localize('UTC')

    # Convert the close_time to Morocco timezone
    close_time = result_data['close_time'].iloc[-1].astimezone(morocco_tz)

    # Check if the seconds part of the time is 59, then add 1 second and adjust time
    if close_time.second == 59:
        close_time += timedelta(seconds=1)
        close_time = close_time.replace(second=0)  # Set seconds to 00 after incrementing

    # Format the close_time without the timezone abbreviation
    close_time_str = close_time.strftime('%Y-%m-%d %H:%M:%S')

    # Return the last close_time, close_price, and detected opportunity_type
    return close_time_str, result_data['close_price'].iloc[-1], result_data['opportunity_type'].iloc[-1]
