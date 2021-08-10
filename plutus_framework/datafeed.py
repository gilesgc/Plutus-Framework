from cryptofeed.callback import CandleCallback
from cryptofeed import FeedHandler
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.defines import CANDLES
from pandas import DataFrame, Timestamp

class CandleSeries:
    def create_candle_dataframes(initial_candle_data):
        for symbol, data in initial_candle_data.items():
            for c in data:
                c[0] = Timestamp(c[0] * 1000000)

        return {
            s: DataFrame(data=initial_candle_data[s][:-1], columns=['date','open','high','low','close','volume']) for s in initial_candle_data.keys()
        }

    def add_candle(dataframe, date, open, high, low, close, volume):
        return dataframe.append({
            "date": Timestamp(date * 1000000),
            "open": float(open),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume)
        }, ignore_index=True)

class DataFeed(FeedHandler):
    def __init__(self, timeframe, tickers, initial_candle_data, candle_callback=None, start_callback=None):
        super().__init__()
        self.timeframe = timeframe
        self.candleseriesdict = CandleSeries.create_candle_dataframes(initial_candle_data)
        self.candle_callback = candle_callback

        if tickers:
            self.add_feed(tickers)

        if callable(start_callback):
            start_callback(self.candleseriesdict)

    async def candle_update(self, feed, symbol, start, stop, interval, trades, open_price, close_price, high_price, low_price, volume, closed, timestamp, receipt_timestamp):
        #print(f"Candle: {timestamp} {receipt_timestamp} Feed: {feed} Symbol: {symbol} Start: {start} Stop: {stop} Interval: {interval} Trades: {trades} Open: {open_price} Close: {close_price} High: {high_price} Low: {low_price} Volume: {volume} Candle Closed? {closed}")
        if closed and (int(timestamp) % (60 * self.timeframe)) == 0:
            date = int((timestamp - 60) * 1000)
            date = date - (date % 60000)
            symbol = symbol.replace("-", "/")
            self.candleseriesdict[symbol] = CandleSeries.add_candle(self.candleseriesdict[symbol], date, open_price, high_price, low_price, close_price, volume)

            if callable(self.candle_callback):
                self.candle_callback(symbol, self.candleseriesdict[symbol].copy())

    def add_feed(self, tickers):
        super().add_feed(BinanceFutures(symbols=tickers, channels=[CANDLES], callbacks={CANDLES: CandleCallback(self.candle_update)}))