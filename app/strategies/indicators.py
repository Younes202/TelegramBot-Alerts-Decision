import pandas_ta as ta
from app.strategies.schemas import DataFrameUtils
from app.strategies.exceptions import StrategyError
import pytz
from datetime import timedelta
import pandas as pd
from loguru import logger

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

    def enhanced_strategy(self):
        def strategy_logic(data):
            # Ensure 'close_time' is a proper datetime index
            if not pd.api.types.is_datetime64_any_dtype(data.index):
                logger.error("'close_time' is not set as a proper datetime index.")
                raise ValueError("'close_time' is missing or not properly set.")

            # Now apply the indicators since the datetime index is ensured
            # Calculate RSI (14 period)
            data['RSI'] = ta.rsi(data['close_price'], length=14)

            # Calculate Bollinger Bands (20 period, 2 std deviation)
            bbands = ta.bbands(data['close_price'], length=20, std=2.0)
            data['BB_lower'] = bbands['BBL_20_2.0']
            data['BB_upper'] = bbands['BBU_20_2.0']

            # Calculate Moving Averages (MA)
            data['MA_10'] = ta.sma(data['close_price'], length=10)
            data['MA_50'] = ta.sma(data['close_price'], length=50)

            # Calculate Exponential Moving Averages (EMA)
            data['EMA_9'] = ta.ema(data['close_price'], length=9)
            data['EMA_21'] = ta.ema(data['close_price'], length=21)

            # Calculate MACD
            macd = ta.macd(data['close_price'], fast=12, slow=26, signal=9)
            data['MACD'] = macd['MACD_12_26_9']
            data['MACD_signal'] = macd['MACDs_12_26_9']

            # Calculate VWAP (VWAP requires a datetime index)
            data['VWAP'] = ta.vwap(data['high_price'], data['low_price'], data['close_price'], data['volume'])

            # Buy signal if RSI < 40, price near lower Bollinger Band, and moving averages crossover
            data['opportunity_type'] = data.apply(
                lambda row: "Buy" if row['RSI'] < 40 and row['close_price'] <= row['BB_lower'] 
                            and row['MA_10'] > row['MA_50'] and row['EMA_9'] > row['EMA_21'] 
                            and row['MACD'] > row['MACD_signal'] and row['close_price'] > row['VWAP']
                            else None,
                axis=1
            )

            # Sell signal if RSI > 70, price near upper Bollinger Band, and moving averages crossover downwards
            data['opportunity_type'] = data.apply(
                lambda row: "Sell" if row['RSI'] > 70 or row['close_price'] >= row['BB_upper'] 
                            and row['MA_10'] < row['MA_50'] and row['EMA_9'] < row['EMA_21'] 
                            and row['MACD'] < row['MACD_signal'] and row['close_price'] < row['VWAP']
                            else row['opportunity_type'],
                axis=1
            )

            return data

        # Apply the strategy logic and reset the index to bring 'close_time' back as a column
        result = self._apply_strategy(strategy_logic)
        result.reset_index(inplace=True)  # Ensures 'close_time' is available as a column
        return result





def get_opportunity(data):
    strategy = Strategy(data)
    result_data = strategy.enhanced_strategy()

    # Ensure 'close_time' is back as a column after resetting the index
    if 'close_time' not in result_data.columns:
        logger.error("Error: 'close_time' column is missing after applying strategy.")
        raise ValueError("'close_time' column is missing after applying the strategy.")

    # Localize close_time to UTC
    result_data['close_time'] = result_data['close_time'].dt.tz_localize('UTC')

    # Convert the close_time to Morocco timezone
    close_time = result_data['close_time'].iloc[-1].astimezone(morocco_tz)

    if close_time.second == 59:
        close_time += timedelta(seconds=1)
        close_time = close_time.replace(second=0)

    close_time_str = close_time.strftime('%Y-%m-%d %H:%M:%S')

    return close_time_str, result_data['close_price'].iloc[-1], result_data['opportunity_type'].iloc[-1]





""""
def backtest_by_strategy_admin(strategy, data, stop_loss=0.02, profit_threshold=0.05, initial_capital=10000):
    trades = []
    df = strategy(data).copy()

    # Calculate profit/loss for each trade
    df['profit'] = df['close_price'] - df['open_price']
    df['holding_period_minutes'] = (df['close_time'] - df['open_time']).dt.total_seconds() / 60
    df['profit_percent'] = (df['profit'] / df['open_price']) * 100
    df['capital_amount'] = initial_capital

    # Calculate position size based on initial capital
    df['position_size'] = df['capital_amount'] / df['open_price']

    # Calculate profit/loss in monetary terms
    df['profit_loss'] = df['position_size'] * df['profit']

    # Update capital after each trade
    df['final_capital'] = df['capital_amount'] + df['profit_loss']

    # Calculate stop loss and profit target
    df['stop_loss'] = stop_loss
    df['target_profit_price'] = profit_threshold

    # Determine if the trade hit stop loss or profit target
    df['trigger'] = df.apply(lambda row: 'Profit' if row['profit_loss'] > 0 else 'Stop_Loss', axis=1)

    # Filter out neutral trades
    df = df[df["opportunity_type"].str.contains("Buy|Sell")]

    # Find BUY and SELL opportunities cycles
    cycles = []
    buy_index = None
    for index, row in df.iterrows():
        if "Buy" in row["opportunity_type"]:
            buy_index = index
        elif "Sell" in row["opportunity_type"] and buy_index is not None:
            buy_row = df.loc[buy_index]
            sell_row = row
            holding_period_minutes = (sell_row["close_time"] - buy_row["open_time"]).total_seconds() / 60
            holding_period= f"{holding_period_minutes:.2f} minutes ({holding_period_minutes / 60:.2f} hours)"
            cycle_profit = (sell_row["profit"] + buy_row["profit"]) / 2
            cycles.append([
                buy_row["open_time"], buy_row["open_price"],
                sell_row["close_time"], sell_row["close_price"],
                cycle_profit, holding_period, sell_row["trigger"],
                len(cycles) + 1, buy_row["stop_loss"],
                sell_row["target_profit_price"], sell_row["final_capital"],
                sell_row["profit"] * sell_row["position_size"]
            ])
            buy_index = None

    # Create the final output DataFrame
    columns = [
        "buy_time", "buy_price", "sell_time", "sell_price",
        "profit", "holding_period", "trigger",
        "total_trades", "stop_loss", "profit_threshold",
        "current_amount", "Profit_Cycle"
    ]

    output_df = pd.DataFrame(cycles, columns=columns)
    output_df.set_index("buy_time", inplace=True)

    # Add Gain/Loss Cycle column based on the profit column
    output_df['Gain/Loss Cycle'] = output_df['Profit_Cycle'].apply(lambda x: 'Gain' if x > 0 else 'Loss')

    # Select specific columns for the final output
    selected_columns = [
        "buy_price", "sell_time", "sell_price", "Profit_Cycle",
        "holding_period", "trigger", "total_trades",
        "stop_loss", "profit_threshold", "current_amount", "Gain/Loss Cycle"
    ]
         
    return output_df[selected_columns]

"""