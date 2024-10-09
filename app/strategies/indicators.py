import pandas_ta as ta
from app.strategies.schemas import DataFrameUtils
from app.strategies.exceptions import StrategyError
from loguru import logger

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

    def RSI_Strategy(self):
        def rsi(data):
            data['RSI'] = ta.rsi(data['close_price'], length=7)  # Use a shorter length for more frequent signals
            data['opportunity_type'] = data['RSI'].apply(
                lambda rsi: "Buy" if rsi < 30 else "Sell" if rsi > 70 else None
            )
            return data
        return self._apply_strategy(rsi)

    def BollingerBands_Strategy(self):
        def bbands(data):
            bands = ta.bbands(data['close_price'], length=20)
            data['BB_upper'] = bands['BBU_20_2.0']
            data['BB_middle'] = bands['BBM_20_2.0']
            data['BB_lower'] = bands['BBL_20_2.0']
            data['opportunity_type'] = data.apply(
                lambda row: "Buy" if row['close_price'] <= row['BB_lower'] else "Sell" if row['close_price'] >= row['BB_upper'] else None, 
                axis=1
            )
            return data
        return self._apply_strategy(bbands)

    def Volume_Strategy(self):
        def volume(data):
            data['close_prev'] = data['close_price'].shift(1)
            data['volume_prev'] = data['volume'].shift(1)
            data['opportunity_type'] = data.apply(
                lambda row: "Buy" if row['close_price'] > row['close_prev'] and row['volume'] > row['volume_prev']
                else "Sell" if row['close_price'] < row['close_prev'] and row['volume'] > row['volume_prev']
                else None,
                axis=1
            )
            return data
        return self._apply_strategy(volume)

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

            # Debugging: Log key indicators for the last data points
            logger.debug(f"Latest RSI: {data['RSI'].iloc[-1]}, BB_lower: {data['BB_lower'].iloc[-1]}, BB_upper: {data['BB_upper'].iloc[-1]}, Volume: {data['volume'].iloc[-1]}, Previous Volume: {data['volume_prev'].iloc[-1]}")

            # Determine opportunities based on a combination of signals
            data['opportunity_type'] = data.apply(
                lambda row: "Buy" if row['RSI'] < 30 and row['close_price'] <= row['BB_lower'] and row['volume'] > row['volume_prev']
                else "Sell" if row['RSI'] > 70 and row['close_price'] >= row['BB_upper'] and row['volume'] > row['volume_prev']
                else None,
                axis=1
            )

            # Debugging: Log the detected opportunity for the latest data point
            logger.debug(f"Detected opportunity: {data['opportunity_type'].iloc[-1]} for close price: {data['close_price'].iloc[-1]}")

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

    # Apply the chosen strategy to the data
    result_data = strategy.rsi_bb_volume_strategy()

    # Return the last close_time, close_price, and detected opportunity_type
    return result_data['close_time'].iloc[-1], result_data['close_price'].iloc[-1], result_data['opportunity_type'].iloc[-1]
