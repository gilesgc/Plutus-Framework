import concurrent.futures

class Algo(object):
    TICKERS = []
    TIMEFRAME = "5m"

    def __init__(self, broker):
        self.broker = broker
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def last(self, candleseries):
        return candleseries.iloc[-1]

    def on_start(self, candleseries):
        pass

    def on_fill(self):
        pass

    def on_tick(self):
        pass

    def on_candle(self, symbol, candleseries):
        pass