from ccxt import binance
import concurrent.futures


class Broker(binance):
    def __init__(self, api_key, api_secret):
        super().__init__({
            'enableRateLimit': True,
            'api_key': api_key,
            'secret': api_secret,
            'options': {
               'defaultType': 'future'
            }
        })
        self._equity = 0
        self._executor = concurrent.futures.ThreadPoolExecutor()
        # self.set_sandbox_mode(True)

        self.positions = {}

        self.update_balance()

    # method for when we want to update balance/positions without holding up the thread
    def update_balance(self):
        return self._executor.submit(self.fetch_balance)
        #Thread(target=self.fetch_balance).start()

    def fetch_balance(self, params={}):
        result = super().fetch_balance(params=params)

        if 'total' in result:
            self._equity = float(result['total']['USDT'])
        
        for p in result['info']['positions']:
            if p['symbol'] in self.positions:
                self.positions[p['symbol']].set_data(p)
            else:
                self.positions[p['symbol']] = Broker.Position(p)

        return result

    def cancel_orders(self, orders, symbol):
        for order in orders:
            try:
                self.cancel_order(order, symbol)
            except Exception as e:
                print(e)
                exit()
            orders.remove(order)

    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):
        if not isinstance(symbol, list):
            return {
                symbol: super().fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit, params=params)
            }

        result = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            f = super().fetch_ohlcv
            future_to_symbol = {executor.submit(f, s, timeframe, since, limit, params): s for s in symbol}
            for future in concurrent.futures.as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    result[sym] = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (sym, exc))

        return result

    @property
    def equity(self):
        return self._equity

    def create_order(self, symbol, type, side, amount, price=None, params={}):
        result = super().create_order(symbol, type, side, amount, price=price, params=params)
        self.update_balance()
        return result

    def get_position(self, symbol):
        return self.positions[symbol.replace("-", "").replace("/", "")]

    def position_amount(self, symbol):
        return self.get_position(symbol).positionAmount

    @classmethod
    def parse_timeframe(cls, timeframe):
        return super().parse_timeframe(timeframe) / 60

    class Position:
        def __init__(self, positiondata):
            self.set_data(positiondata)

        def set_data(self, positiondata):
            self.symbol = positiondata['symbol']
            self.initialMargin = float(positiondata['initialMargin'])
            self.maintMargin = float(positiondata['maintMargin'])
            self.unrealizedProfit = float(positiondata['unrealizedProfit'])
            self.positionInitialMargin = float(positiondata['positionInitialMargin'])
            self.openOrderInitialMargin = float(positiondata['openOrderInitialMargin'])
            self.leverage = int(positiondata['leverage'])
            self.isIsolated = positiondata['isolated']
            self.entryPrice = float(positiondata['entryPrice'])
            self.maxNotional = float(positiondata['maxNotional'])
            self.positionSide = positiondata['positionSide']
            self.positionAmount = float(positiondata['positionAmt'])
            self.notional = float(positiondata['notional'])

            return self