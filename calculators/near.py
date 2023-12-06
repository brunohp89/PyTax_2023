import numpy as np
from PricesClass import Prices
import tax_library as tx
import datetime as dt
import pandas as pd
import requests
import json
from dateutil.relativedelta import relativedelta

def get_transactions_df(address):
    address = address.lower()
    normal_df = pd.DataFrame()

    url = f'https://api.nearblocks.io/v1/account/{address}/txns?page=1&per_page=25&order=desc'
    trx_df = pd.DataFrame(json.loads(requests.get(url).content)['txns'])
    normal_df = pd.concat([trx_df, normal_df])
    i = 1
    while trx_df.shape[0] > 0 and i < 150:
        url = f'https://api.nearblocks.io/v1/account/{address}/txns?page={i}&per_page=25&order=desc'
        trx_df = pd.DataFrame(json.loads(requests.get(url).content)['txns'])
        normal_df = pd.concat([trx_df, normal_df])
        i += 1

    nep21_df = pd.DataFrame()
    url = f'https://api.nearblocks.io/v1/account/{address}/ft-txns?page=1&per_page=25&order=desc'
    trx_df = pd.DataFrame(json.loads(requests.get(url).content)['txns'])
    nep21_df = pd.concat([trx_df, nep21_df])
    i = 1
    while trx_df.shape[0] > 0 and i < 150:
        url = f'https://api.nearblocks.io/v1/account/{address}/ft-txns?page={i}&per_page=25&order=desc'
        trx_df = pd.DataFrame(json.loads(requests.get(url).content)['txns'])
        nep21_df = pd.concat([trx_df, nep21_df])
        i += 1

    normal_df = normal_df[~normal_df.transaction_hash.isin(nep21_df.transaction_hash)]
    normal_df.index = [dt.datetime.fromtimestamp(int(int(x)/10**9)) for x in normal_df['block_timestamp']]
    normal_df = normal_df.sort_index()
    normal_df['Fee'] = [-int(k['transaction_fee'])/10**24 for k in normal_df['outcomes_agg']]
    try:
        normal_df['content'] = [json.loads(k[0].replace("\\","").replace("EVENT_JSON:","")) if len(k) > 0 else None for k in normal_df['logs']]
    except:
        normal_df['content'] = normal_df['logs']
    normal_df['From'] = normal_df['predecessor_account_id']#[x['data'][0]['old_owner_id'] if x is not None else x for x in normal_df['content']]
    normal_df['To'] = normal_df['receiver_account_id']#[x['data'][0]['new_owner_id'] if x is not None else x for x in normal_df['content']]

    normal_df['amount'] = [k['deposit']/10**24 for k in normal_df['actions_agg']]
    normal_df.loc[normal_df['predecessor_account_id'] == address, 'amount'] *= -1
    normal_df.loc[normal_df['receiver_account_id'] == address, 'Fee'] *= 0
    normal_df.loc[normal_df['amount'] < 0, 'From Amount'] = normal_df.loc[normal_df['amount'] < 0, 'amount']
    normal_df.loc[normal_df['amount'] > 0, 'To Amount'] = normal_df.loc[normal_df['amount'] > 0, 'amount']

    normal_df.loc[normal_df['amount'] < 0, 'From Coin'] = 'NEAR'
    normal_df.loc[normal_df['amount'] > 0, 'To Coin'] = 'NEAR'

    normal_df['Fee Coin'] = 'NEAR'
    normal_df.loc[normal_df['Fee'] == 0,'Fee'] = None
    normal_df['Tag'] = 'Movement'
    normal_df['Notes'] = [str(x) for x in normal_df['actions']]
    normal_df = normal_df.drop(['receipt_id', 'predecessor_account_id', 'receiver_account_id',
       'transaction_hash', 'included_in_block_hash', 'block_timestamp',
       'block', 'actions', 'actions_agg', 'outcomes', 'outcomes_agg', 'logs', 'content', 'amount'],axis=1)
    normal_df['Fiat'] = 'EUR'
    normal_df['Source'] = f'NEAR - {address[0:10]}'
    normal_df[
        ["Fee Fiat",
        "Fiat Price"]
    ] = None
    normal_df = normal_df.drop_duplicates()


    nep21_df.index = [dt.datetime.fromtimestamp(int(int(x) / 10 ** 9)) for x in nep21_df['block_timestamp']]
    nep21_df = nep21_df.sort_index()
    nep21_df.loc[nep21_df.token_old_owner_account_id==address,'Fee'] = -0.01 # Might be wrong but fee is not being transmitted
    nep21_df['token'] = [x['name'] for x in nep21_df.ft]
    nep21_df['decimals'] = [x['decimals'] for x in nep21_df.ft]
    nep21_df['amount'] = [int(x)/(10**int(y)) for x,y in zip(nep21_df['amount'],nep21_df['decimals'])]

    nep21_df.loc[nep21_df.token_old_owner_account_id==address, 'amount'] *= -1
    nep21_df.loc[nep21_df.token_old_owner_account_id == 'jars.sweat', ['Tag', 'Notes']] = ['Reward','SWEAT Flexible Jars']
    nep21_df.loc[nep21_df.token_new_owner_account_id == 'jars.sweat', 'amount'] = None

    nep21_df.loc[nep21_df.token_old_owner_account_id == 'spin.sweat', ['Tag', 'Notes']] = ['Reward','SWEAT Spin Wheel']
    nep21_df.loc[nep21_df.token_new_owner_account_id == 'spin.sweat', ['Tag', 'Notes']] = ['Movement', 'SWEAT Spin Wheel']

    nep21_df.loc[nep21_df['event_kind']=='MINT', ['Tag', 'Notes']] = ['Reward', 'SWEAT Steps Earn']
    nep21_df.loc[nep21_df.token_new_owner_account_id == 'reward-optin.sweat', ['Tag', 'Notes']] = ['Movement', 'SWEAT Optin Prize']
    nep21_df.loc[nep21_df.token_new_owner_account_id == 'fees.sweat', ['Tag', 'Notes']] = ['Movement', 'SWEAT Fees']
    nep21_df.loc[nep21_df.token_new_owner_account_id == 'governance.sweat', ['Tag', 'Notes']] = ['Movement', 'SWEAT Governance Voting']

    jars_df = nep21_df[np.logical_or(nep21_df.token_new_owner_account_id == 'deposits.grow.sweat',nep21_df.token_old_owner_account_id == 'distributions.grow.sweat' )]
    if jars_df.shape[0] > 0:
        nep21_df.loc[np.logical_or(nep21_df.token_new_owner_account_id == 'deposits.grow.sweat',nep21_df.token_old_owner_account_id == 'distributions.grow.sweat' ), 'drop'] = 1
        nep21_df = nep21_df[nep21_df['drop'] != 1]
        jars_df.index = [k-dt.timedelta(seconds=k.second) for k in jars_df.index]
        for optout in jars_df[jars_df.amount > 0].index:
            one_month = optout - relativedelta(months=1)
            three_months = optout - relativedelta(months=3)
            six_months = optout - relativedelta(months=6)
            twelve_months = optout - relativedelta(months=12)
            if len(jars_df.loc[
                       jars_df.index.isin([one_month - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount']) > 0:
                jars_df.loc[jars_df.index == optout, 'amount'] = jars_df.loc[jars_df.index.isin([one_month-dt.timedelta(minutes=x) for x in range(-5,5)]), 'amount'].values[0] + jars_df.loc[jars_df.index == optout, 'amount'].values[0]
                jars_df.loc[jars_df.index.isin([one_month-dt.timedelta(minutes=x) for x in range(-5,5)]), 'amount'] = None

            if len(jars_df.loc[jars_df.index.isin([three_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount']) > 0:
                jars_df.loc[jars_df.index == optout, 'amount'] = \
                jars_df.loc[jars_df.index.isin([three_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount'].values[
                    0] + jars_df.loc[jars_df.index == optout, 'amount'].values[0]
                jars_df.loc[jars_df.index.isin([three_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount'] = None

            if len(jars_df.loc[jars_df.index.isin(
                        [six_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount']) > 0:

                jars_df.loc[jars_df.index == optout, 'amount'] = \
                jars_df.loc[jars_df.index.isin([six_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount'].values[
                    0] + jars_df.loc[jars_df.index == optout, 'amount'].values[0]
                jars_df.loc[jars_df.index.isin([six_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount'] = None

            if len(jars_df.loc[jars_df.index.isin(
                        [twelve_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount']) > 0:

                jars_df.loc[jars_df.index == optout, 'amount'] = \
                jars_df.loc[jars_df.index.isin([twelve_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount'].values[
                    0] + jars_df.loc[jars_df.index == optout, 'amount'].values[0]
                jars_df.loc[jars_df.index.isin([twelve_months - dt.timedelta(minutes=x) for x in range(-5, 5)]), 'amount'] = None
        jars_df.loc[np.logical_or(jars_df['amount'] < 0,pd.isna(jars_df['amount'])), ['Tag','Notes']] = ['Movement', 'SWEAT Grow Jar']
        jars_df.loc[jars_df['amount'] > 0, ['Tag', 'Notes']] = ['Reward', 'SWEAT Grow Jar']
        jars_df.loc[jars_df['amount'] < 0, 'amount'] = None

        nep21_df = pd.concat([nep21_df,jars_df])
        nep21_df = nep21_df.drop('drop',axis=1)
    nep21_df.loc[nep21_df['amount'] < 0, 'From Amount'] = nep21_df.loc[nep21_df['amount'] < 0, 'amount']
    nep21_df.loc[nep21_df['amount'] > 0, 'To Amount'] = nep21_df.loc[nep21_df['amount'] > 0, 'amount']

    nep21_df.loc[nep21_df['amount'] < 0, 'From Coin'] = nep21_df.loc[nep21_df['amount'] < 0, 'token']
    nep21_df.loc[nep21_df['amount'] > 0, 'To Coin'] = nep21_df.loc[nep21_df['amount'] > 0, 'token']

    nep21_df[['From','To']] = nep21_df[['token_old_owner_account_id', 'token_new_owner_account_id']]
    nep21_df = nep21_df.drop(['key', 'token_old_owner_account_id', 'token_new_owner_account_id',
       'amount', 'event_kind', 'transaction_hash', 'included_in_block_hash',
       'block_timestamp', 'block', 'outcomes', 'ft', 'token',
       'decimals'],axis=1)
    nep21_df['Fiat'] = 'EUR'
    nep21_df['Fee Coin'] = 'NEAR'
    nep21_df['Source'] = f'NEAR - {address[0:10]}'
    nep21_df[
        ["Fee Fiat",
        "Fiat Price"]
    ] = None
    nep21_df = nep21_df.drop_duplicates()

    outdf = pd.concat([normal_df, nep21_df]).sort_index()
    outdf = outdf[
        [
            "From",
            "To",
            "From Coin",
            "To Coin",
            "From Amount",
            "To Amount",
            "Fee",
            "Fee Coin",
            "Fee Fiat",
            "Fiat",
            "Fiat Price",
            "Tag",
            "Source",
            "Notes",
        ]
    ]

    outdf = tx.price_transactions_df(outdf, Prices())
    return outdf
