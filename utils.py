import pickle as pk
import contextlib
import pandas as pd
import datetime as dt


def save_pickle(obj, name):
    with open(name, "wb") as handle:
        pk.dump(obj, handle, protocol=pk.HIGHEST_PROTOCOL)


def open_pickle(name):
    with open(name, "rb") as handle:
        vout = pk.load(handle)
    return vout


def calculate_pmc(coin, transactions):
    import numpy as np
    import tax_library as tx
    from PricesClass import Prices
    coin = coin.upper()

    coin_df = transactions[np.logical_or(transactions['To Coin'] == coin, transactions['From Coin'] == coin)].copy()
    coin_df.loc[coin_df['To Coin'] == '', 'To Coin'] = None
    coin_df.loc[coin_df['From Coin'] == '', 'From Coin'] = None

    if any(pd.isna(coin_df['Fiat Price'])):
        print(f'Calculating {coin}')
        with open('log.txt', 'w') as f:
            with contextlib.redirect_stdout(f):
                coin_df = tx.price_transactions_df(coin_df, Prices())

    if 'PIT' in coin_df['To Coin'].tolist() or 'PIT' in coin_df['From Coin'].tolist():
        print('Just a little longer')
        with open('log.txt', 'w') as f:
            with contextlib.redirect_stdout(f):
                to_price = coin_df.loc[np.logical_or(coin_df['To Coin'] == 'PIT', coin_df['From Coin'] == 'PIT')].copy()
                to_price.loc[to_price['To Coin'] == 'PIT', 'To Coin'] = None
                to_price.loc[to_price['From Coin'] == 'PIT', 'From Coin'] = None
                to_price['Fiat Price'] = None
                to_price = tx.price_transactions_df(to_price, Prices())
                coin_df.loc[np.logical_or(coin_df['To Coin'] == 'PIT', coin_df['From Coin'] == 'PIT'), 'Fiat Price'] = \
                    to_price['Fiat Price']

    coin_df.loc[np.logical_and(pd.isna(coin_df['To Coin']),
                               coin_df['Tag'].str.contains('ERC721', na=False)), 'To Coin'] = 'NFT Exchange'
    coin_df.loc[np.logical_and(pd.isna(coin_df['From Coin']),
                               coin_df['Tag'].str.contains('ERC721', na=False)), 'From Coin'] = 'NFT Exchange'
    coin_df.loc[np.logical_and(pd.isna(coin_df['To Coin']),
                               coin_df['Tag'].str.contains('ERC1155', na=False)), 'To Coin'] = 'NFT Exchange'
    coin_df.loc[np.logical_and(pd.isna(coin_df['From Coin']),
                               coin_df['Tag'].str.contains('ERC1155', na=False)), 'From Coin'] = 'NFT Exchange'

    coin_df2 = coin_df[np.logical_and(~pd.isna(coin_df['To Coin']), ~pd.isna(coin_df['From Coin']))]
    coin_df3 = coin_df[coin_df['Tag'].isin(['Interest', 'Reward', 'Cashback'])]

    coin_df = pd.concat([coin_df2, coin_df3])

    if coin_df[coin_df['To Amount'] < 0].shape[0] > 0:
        raise TabError("Found negative values in to amount")
    if coin_df[coin_df['From Amount'] > 0].shape[0] > 0:
        raise TabError("Found positive values in from amount")

    fee_df = transactions[transactions['Fee Coin'] == coin].copy()
    fee_df = fee_df[['Fee', 'Fee Fiat']]
    fee_df.columns = ['Amount', 'Fiat Price']

    coin_df.loc[coin_df['To Coin'] == '', 'To Coin'] = None
    coin_df.loc[coin_df['From Coin'] == '', 'From Coin'] = None

    coin_df['From Amount'] = coin_df['From Amount'].fillna(0)
    coin_df['To Amount'] = coin_df['To Amount'].fillna(0)

    coin_df.loc[coin_df['From Coin'] == coin, 'Amount'] = coin_df.loc[coin_df['From Coin'] == coin, 'From Amount']
    coin_df.loc[coin_df['To Coin'] == coin, 'Amount'] = coin_df.loc[coin_df['To Coin'] == coin, 'To Amount']

    coin_df['Fee Fiat'] = coin_df['Fee Fiat'].fillna(0)
    coin_df.loc[np.logical_and(coin_df['Fee Coin'] != coin, coin_df['From Coin'] == coin), 'Fiat Price'] += coin_df.loc[
        np.logical_and(coin_df['Fee Coin'] != coin, coin_df['From Coin'] == coin), 'Fee Fiat']
    coin_df.loc[np.logical_and(coin_df['Fee Coin'] != coin, coin_df['To Coin'] == coin), 'Fiat Price'] += coin_df.loc[
        np.logical_and(coin_df['Fee Coin'] != coin, coin_df['To Coin'] == coin), 'Fee Fiat']

    coin_df = pd.concat([coin_df[['Amount', 'Fiat Price']], fee_df[['Amount', 'Fiat Price']]])
    coin_df['Amount'] = coin_df['Amount'].fillna(0)
    coin_df['Fiat Price'] = coin_df['Fiat Price'].fillna(0)

    coin_df['Sign'] = [np.sign(x) for x in coin_df['Amount']]
    coin_df['Fiat Price'] = coin_df['Fiat Price'].abs()
    coin_df['Fiat Price'] *= coin_df['Sign']
    coin_df = coin_df.sort_index()

    prezzo_acquisto = coin_df.loc[coin_df['Amount'] > 0, 'Fiat Price'].sum() / coin_df.loc[
        coin_df['Amount'] > 0, 'Amount'].sum()
    prezzo_vendita = coin_df.loc[coin_df['Amount'] < 0, 'Fiat Price'].sum() / coin_df.loc[
        coin_df['Amount'] < 0, 'Amount'].sum()
    primo_acq = min(coin_df.loc[coin_df['Amount'] > 0].index)
    ultimo_acq = max(coin_df.loc[coin_df['Amount'] > 0].index)

    if coin_df.loc[coin_df['Amount'] < 0].shape[0] > 0:
        prima_ven = min(coin_df.loc[coin_df['Amount'] < 0].index)
        ultima_ven = max(coin_df.loc[coin_df['Amount'] < 0].index)
    else:
        prima_ven = ultima_ven = None

    return {
        f"Prezzo Medio d'Acquisto {coin}": prezzo_acquisto,
        f"Prezzo Medio di Vendita {coin}": prezzo_vendita,
        f'Quantità acquistata {coin}': coin_df.loc[coin_df['Amount'] > 0, 'Amount'].sum(),
        f'Quantità venduta {coin}': coin_df.loc[coin_df['Amount'] < 0, 'Amount'].sum(),
        f'Primo acquisto {coin}': primo_acq,
        f'Prima vendita {coin}': prima_ven,
        f'Ultimo acquisto {coin}': ultimo_acq,
        f'Ultima vendita {coin}': ultima_ven
    }


def date_from_timestamp(x):
    time_now_sys = dt.datetime.strptime(dt.datetime.now(dt.datetime.now().astimezone().tzinfo).strftime('%Y-%m-%d '
                                                                                                        '%H:%M:%S'),
                                        '%Y-%m-%d %H:%M:%S')
    time_now_utc = dt.datetime.strptime(dt.datetime.now(dt.UTC).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    diff_utc = -round(((time_now_sys - time_now_utc).seconds / 60) / 60)
    return dt.datetime.fromtimestamp(int(x)) + dt.timedelta(hours=diff_utc)
