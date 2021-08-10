from .broker import Broker
from .datafeed import DataFeed
from .algo import Algo
import pandas as pd
import numpy as np

class Plutus(object):
    def __init__(self, api_key, api_secret):
        self.broker = Broker(api_key, api_secret)
        self.strategies = {}

    def set_strategy(self, strategy):
        assert isinstance(strategy, type) and issubclass(strategy, Algo)
        self.strategy = strategy
        self.strategies = {sym: strategy(self.broker) for sym in strategy.TICKERS}

    def run(self):
        self.datafeed = DataFeed(
            timeframe=Broker.parse_timeframe(self.strategy.TIMEFRAME),
            tickers=[t.replace("/", "-") for t in self.strategy.TICKERS],
            initial_candle_data=self.broker.fetch_ohlcv(self.strategy.TICKERS),
            candle_callback=self.candle_callback,
            start_callback=self.start_callback
        )

        self.datafeed.run()

    def candle_callback(self, symbol, candleseries):
        self.strategies[symbol].on_candle(symbol, candleseries)

    def start_callback(self, candleseries):
        pass

class PlutusLib:
    # https://github.com/ranaroussi/qtpylib/blob/main/qtpylib/indicators.py

    def crossed(series1, series2, direction=None):
        if isinstance(series1, np.ndarray):
            series1 = pd.Series(series1)

        if isinstance(series2, (float, int, np.ndarray)):
            series2 = pd.Series(index=series1.index, data=series2)

        if direction is None or direction == "above":
            above = pd.Series((series1 > series2) & (
                series1.shift(1) <= series2.shift(1)))

        if direction is None or direction == "below":
            below = pd.Series((series1 < series2) & (
                series1.shift(1) >= series2.shift(1)))

        if direction is None:
            return above or below

        return above if direction == "above" else below


    def crossed_above(series1, series2):
        return PlutusLib.crossed(series1, series2, "above")


    def crossed_below(series1, series2):
        return PlutusLib.crossed(series1, series2, "below")
