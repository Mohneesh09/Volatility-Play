import asyncio
import json
import pandas as pd
import websockets
from dateutil import parser
import datetime as dt


class DeribitOptionsData:
    def __init__(self, instrument):
        instrument = instrument.upper()
        if instrument not in ['BTC', 'ETH']:
            raise ValueError('instrument must be either BTC or ETH')
        self._instrument = instrument
        self.options = None
        self._get_options_chain()
        self.call_time = dt.datetime.now()

    @staticmethod
    async def call_api(msg):
        async with websockets.connect('wss://www.deribit.com/ws/api/v2') as websocket:
            await websocket.send(msg)
            while websocket.open:
                response = await websocket.recv()
                return response

    @staticmethod
    def json_to_dataframe(response):
        response = json.loads(response)
        results = response['result']
        df = pd.DataFrame(results)
        return df

    def update(self):
        self._get_options_chain()

    @property
    def instrument(self):
        return self._instrument

    @instrument.setter
    def instrument(self, new_instrument):
        if isinstance(new_instrument, str):
            self._instrument = new_instrument
        else:
            raise ValueError('New instrument must be a string')

    @staticmethod
    def date_parser(str_date):
        date = str_date.split('-')[-1]
        return parser.parse(date)

    @staticmethod
    def strike_parser(inst_name):
        strike = inst_name.split('-')[-2]
        return int(strike)

    @staticmethod
    def side_parser(inst_name):
        side = inst_name.split('-')[-1]
        if side == 'P':
            return 'Put'
        if side == 'C':
            return 'Call'
        else:
            return 'N/A'

    @staticmethod
    def async_loop(message):
        return asyncio.get_event_loop().run_until_complete(DeribitOptionsData.call_api(message))


    def process_df(self, df):
        # add expiry column
        df['expiry'] = [DeribitOptionsData.date_parser(date) for date in df.underlying_index]
        #add strike column
        df['strike'] = [DeribitOptionsData.strike_parser(i) for i in df.instrument_name]

        # add side, i.e. put or call
        df['type'] = [DeribitOptionsData.side_parser(j) for j in df.instrument_name]

        # get example option with closest expiry in order to calc dollar price of options_trading

        # spot = DeribitOptionsData.get_quote(self._instrument)

        df['dollar_bid'] = df.underlying_price * df.bid_price
        df['dollar_ask'] = df.underlying_price * df.ask_price
        df['dollar_mid'] = df.underlying_price * df.mid_price

        #create time to expiry as float
        df['time'] = (df['expiry'] - dt.datetime.now()).dt.days / 365

        return df

    @staticmethod
    def get_quote(instrument):
        msg1 = \
            {"jsonrpc": "2.0","id": 9344,"method": "public/ticker",
                "params": {"instrument_name": instrument +'-PERPETUAL',"kind": "future"}
            }
        quote = json.loads(DeribitOptionsData.async_loop(json.dumps(msg1)))
        return float(quote['result']['last_price'])

    @property
    def chain(self):
        return self.options

    def _get_options_chain(self):
        msg1 = \
            {
                "jsonrpc": "2.0","id": 9344,"method": "public/get_book_summary_by_currency",
                "params": {"currency": self._instrument,"kind": "option"}
            }

        response = self.async_loop(json.dumps(msg1))
        df = self.json_to_dataframe(response)
        df = self.process_df(df)
        self.options = df.copy(deep=True)

    def available_instruments(self, currency, expired=False):
        msg = \
            {
                "jsonrpc": "2.0","id": 9344,"method": "public/get_instruments",
                "params": {"currency": currency,"kind": "option","expired": expired}
            }
        resp = self.async_loop(json.dumps(msg))
        resp = json.loads(resp)
        instruments = [d["instrument_name"] for d in resp['result']]
        return instruments

    @classmethod
    def option_info(cls, option_label):
        msg = \
            {
                "jsonrpc": "2.0","id": 8106,"method": "public/ticker",
                "params": {"instrument_name": option_label}
            }

        response = DeribitOptionsData.async_loop(json.dumps(msg))
        return json.loads(response)

    def expiries(self):
        return sorted(self.options.expiry.unique())

    def get_side_exp(self, side, exp='all'):
        if side.capitalize() not in ['Call', 'Put']:
            raise ValueError("Side must be 'Call' or 'Put'")
        if exp == 'all':
            return self.options[self.options['type'] == side]
        else:
            return self.options[(self.options.expiry == exp) & (self.options['type'] == side)]


optObj = DeribitOptionsData('BTC')
opts = optObj.options

#print(opts.columns)

exp = optObj.expiries()
#print(exp)

calls = optObj.get_side_exp('Call', exp[-2])
print(calls[['strike', 'dollar_bid', 'dollar_mid', 'dollar_ask', 'time']])
df=calls[['strike', 'dollar_bid', 'dollar_mid', 'dollar_ask', 'time']]
#print(type(calls))
#df.to_csv('options_BTC_PUT.csv')
df.to_csv('options_BTC_CALL.csv')


calls = optObj.get_side_exp('Put', exp[-2])
print(calls[['strike', 'dollar_bid', 'dollar_mid', 'dollar_ask', 'time']])
df=calls[['strike', 'dollar_bid', 'dollar_mid', 'dollar_ask', 'time']]
#print(type(calls))
df.to_csv('options_BTC_PUT.csv')
