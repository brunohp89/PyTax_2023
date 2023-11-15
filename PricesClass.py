from typing import List
import numpy as np
import pandas as pd
import pickle as pk
import yfinance as yf
import datetime as dt
import os
from utils import save_pickle, open_pickle


class Prices:
    def __init__(self):
        if "prices.pickle" not in os.listdir():
            self.prices = {"Prices": {"USD": {}}}
        else:
            self.prices = open_pickle("prices.pickle")

        if f"exchange_rates.pickle" not in os.listdir():
            self.exchange_rates = dict()
        else:
            with open(f"exchange_rates.pickle", "rb") as prices_list:
                self.exchange_rates = pk.load(prices_list)

    def call_prices(self, symbol):
        if symbol.upper() == "STG":
            exc_rate = yf.Ticker("STG18934-USD")
        elif symbol.upper() == "LUNA":
            exc_rate = yf.Ticker("LUNA20314-USD")
        elif symbol.upper() == "NFT":
            exc_rate = yf.Ticker("NFT9816-USD")
        elif symbol.upper() == "APE":
            exc_rate = yf.Ticker("APE18876-USD")
        elif symbol.upper() == "HFT":
            exc_rate = yf.Ticker("HFT22461-USD")
        elif symbol.upper() == 'GMT':
            exc_rate = yf.Ticker("GMT18069-USD")
        elif symbol.upper() == 'SUI':
            exc_rate = yf.Ticker("SUI20947-USD")
        elif symbol.upper() == 'GST':
            exc_rate = yf.Ticker("GST16352-USD")
        elif symbol.upper() == 'LOVE':
            exc_rate = yf.Ticker("LOVE26337-USD")
        elif symbol.upper() == 'ARGO' or symbol.upper() == 'XARGO':
            exc_rate = yf.Ticker("ARGO19900-USD")
        elif symbol.upper() == 'GENE':
            exc_rate = yf.Ticker("GENE13632-USD")
        elif symbol.upper() == 'LOVE':
            exc_rate = yf.Ticker("LOVE26337-USD")
        else:
            exc_rate = yf.Ticker(f"{symbol.upper()}-USD")
        exc_rate_history = exc_rate.history(period="max")
        exc_rate_history = exc_rate_history[["Open", "Close", "High", "Low"]].copy()
        exc_rate_history.ffill(inplace=True)
        if "+" in str(exc_rate_history.index[0]):
            exc_rate_history.index = [
                str(x).split("+")[0] for x in exc_rate_history.index
            ]
        exc_rate_history.index = [
            dt.datetime.strptime(
                str(exc_rate_history.iloc[x, :].name), "%Y-%m-%d %H:%M:%S"
            ).date()
            for x in range(exc_rate_history.shape[0])
        ]
        if exc_rate_history.shape[0] == 0:
            self.prices["Prices"]["USD"][symbol.upper()] = {}
            self.prices["Prices"]["USD"][symbol.upper()]["Values"] = None
        else:
            self.prices["Prices"]["USD"][symbol.upper()] = {}
            self.prices["Prices"]["USD"][symbol.upper()]["Values"] = exc_rate_history
        self.prices["Prices"]["USD"][symbol.upper()]["LastUpdate"] = dt.date.today()

    def get_prices(self, symbol_list: List[str], force_update=False):
        for symbol in symbol_list:
            if symbol.upper() in self.prices["Prices"]["USD"].keys():
                if (
                    self.prices["Prices"]["USD"][symbol.upper()]["LastUpdate"]
                    < dt.date.today()
                ):
                    print(f"Getting prices for {symbol.upper()}")
                    self.call_prices(symbol)
                else:
                    if not force_update:
                        print(f"{symbol.upper()} prices already updated")
                if force_update:
                    print(f"Getting prices for {symbol.upper()}")
                    self.call_prices(symbol)
            else:
                print(f"Getting prices for {symbol.upper()}")
                self.call_prices(symbol)
        save_pickle(self.prices, "prices.pickle")

    def get_exchange_rates(self, currency):
        exc_rate = yf.Ticker(f"{currency.upper()}=X")
        exc_rate_history = exc_rate.history(period="max")

        if "+" in str(exc_rate_history.index[0]):
            exc_rate_history.index = [
                str(x).split("+")[0] for x in exc_rate_history.index
            ]

        exc_rate_index = [
            dt.datetime.strptime(
                str(exc_rate_history.iloc[x, :].name), "%Y-%m-%d %H:%M:%S"
            ).date()
            for x in range(exc_rate_history.shape[0])
        ]
        exc_rate_vals = [
            float(exc_rate_history.loc[exc_rate_history.index[x], "Close"])
            for x in range(exc_rate_history.shape[0])
        ]

        exc_rate_df = pd.DataFrame()
        exc_rate_df["vals"] = exc_rate_vals
        exc_rate_df.index = exc_rate_index

        placeholder_index = [
            (exc_rate_df.index[0] + dt.timedelta(days=x))
            for x in range(1, (exc_rate_df.index[-1] - exc_rate_df.index[0]).days)
        ]
        placeholder_df = pd.DataFrame()
        placeholder_df.index = placeholder_index
        placeholder_df["vals"] = np.NaN

        joined_df = placeholder_df.join(exc_rate_df, lsuffix="L-", how="outer")
        joined_df.drop([joined_df.columns[0]], axis=1, inplace=True)
        joined_df.ffill(inplace=True)

        exc_rate_list = [
            (joined_df.index[x], joined_df["vals"][x])
            for x in range(joined_df.shape[0])
        ]

        if exc_rate_list[-1][0] == (dt.date.today() - dt.timedelta(days=2)):
            exc_rate_list.append(
                ((dt.date.today() - dt.timedelta(days=1)), exc_rate_list[-1][1])
            )

        self.exchange_rates[currency.upper()] = exc_rate_list
        save_pickle(self.exchange_rates, "exchange_rates.pickle")

    def convert_prices(self, token_list, currency):
        self.get_exchange_rates(currency)

        if currency.upper() not in self.prices["Prices"].keys():
            self.prices["Prices"][currency.upper()] = dict()

        for token in token_list:
            print(f"Converting prices of {token.upper()} to {currency.upper()}")
            token = token.upper()

            if token not in self.prices["Prices"]["USD"].keys():
                self.get_prices([token])
            currency_pd = pd.DataFrame(self.exchange_rates[currency.upper()])
            currency_pd.index = currency_pd.iloc[:, 0]
            currency_pd = currency_pd.iloc[:, [1]].copy()
            currency_pd.columns = ["Convert"]
            token_pd = pd.merge(
                self.prices["Prices"]["USD"][token.upper()]["Values"],
                currency_pd,
                left_index=True,
                right_index=True,
            )
            token_pd["Open"] = [
                x * y for x, y in zip(token_pd["Convert"], token_pd["Open"])
            ]
            token_pd["High"] = [
                x * y for x, y in zip(token_pd["Convert"], token_pd["High"])
            ]
            token_pd["Low"] = [
                x * y for x, y in zip(token_pd["Convert"], token_pd["Low"])
            ]
            token_pd["Close"] = [
                x * y for x, y in zip(token_pd["Convert"], token_pd["Close"])
            ]

            self.prices["Prices"][currency.upper()][token.upper()] = token_pd
        save_pickle(self.prices, "prices.pickle")