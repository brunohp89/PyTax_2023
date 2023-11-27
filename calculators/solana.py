from PricesClass import Prices
import datetime as dt
import pandas as pd
import requests
import tax_library as tx
import os
import numpy as np
import json
import time
import pickle as pk
import math

scam = ["6GnNtx93PwLwxQJtdW3g1kUpbqFvxRWqNmyqdxHG5yTV"]


def get_transactions_df(address):
    nfts = []
    transactions=requests.get(f'https://api.solana.fm/v0/accounts/{address}/transactions?utcFrom=1611839871&utcTo={int(dt.datetime.now().timestamp())}')
    data = [x['signature'] for x in json.loads(transactions.text)['result']['data']]

    transactions_content = []
    for i in range(math.ceil(len(data)/50)):
        if (i+1)*50 > len(data):
            end = len(data)
        else:
            end = (i+1)*50
        start = i*50
        transactions_content.extend(requests.post('https://api.solana.fm/v0/transactions', json={"transactionHashes":data[start:end]}).json()['result'])

    response_pd = pd.DataFrame(transactions_content)

    response_pd["From"] = None
    response_pd["To"] = None
    response_pd["From Amount"] = None
    response_pd["From Coin"] = None
    response_pd["To Amount"] = None
    response_pd["To Coin"] = None
    response_pd["Fee"] = None
    response_pd["Fee Coin"] = None
    response_pd["Fee Fiat"] = None
    response_pd["Tag"] = None
    response_pd["Notes"] = ""
    response_pd["Fiat Price"] = None
    response_pd.index = response_pd["data"].apply(lambda x: dt.datetime.fromtimestamp(int(x["blockTime"])))
    response_pd = response_pd.sort_index()
    for transaction in response_pd['data']:
        if 'HarvestKi' in ','.join(transaction["meta"]["logMessages"]):
            response_pd.loc[response_pd['data'] == transaction, "From"] = address
            response_pd.loc[response_pd['data'] == transaction, "To"] = transaction['transaction']["message"]['accountKeys'][-1]['pubkey']
            response_pd.loc[response_pd['data'] == transaction, "From Amount"] = -transaction["meta"]["innerInstructions"][0]['instructions'][0]['parsed']['info']['lamports']/10**9
            response_pd.loc[response_pd['data'] == transaction, "From Coin"] = 'SOL'
            response_pd.loc[response_pd['data'] == transaction, "Fee"] = -transaction["meta"]["fee"]/10**9
            response_pd.loc[response_pd['data'] == transaction, "Fee Coin"] = 'SOL'
            response_pd.loc[response_pd['data'] == transaction, "Notes"] = 'Genopet Harvest Ki'
        elif 'WithdrawKi' in ','.join(transaction["meta"]["logMessages"]):
            response_pd.loc[response_pd['data'] == transaction, "To"] = address
            response_pd.loc[response_pd['data'] == transaction, "From"] = transaction['transaction']["message"]['accountKeys'][-1]['pubkey']
            if [k for k in transaction['meta']["preTokenBalances"] if k['owner'] == address][0]['uiTokenAmount']['uiAmount'] is not None:
                response_pd.loc[response_pd['data'] == transaction, "To Amount"] = [k for k in transaction['meta']["postTokenBalances"] if k['owner'] == address][0]['uiTokenAmount']['uiAmount'] - [k for k in transaction['meta']["preTokenBalances"] if k['owner'] == address][0]['uiTokenAmount']['uiAmount']
            else:
                response_pd.loc[response_pd['data'] == transaction, "To Amount"] = [k for k in transaction['meta']["postTokenBalances"] if k['owner'] == address][0]['uiTokenAmount']['uiAmount']
            response_pd.loc[response_pd['data'] == transaction, "Notes"] = 'Genopet Mint Ki'
            response_pd.loc[response_pd['data'] == transaction, "To Coin"] = 'KI'
            response_pd.loc[response_pd['data'] == transaction, "Fee"] = -transaction["meta"]["fee"]/10**9
            response_pd.loc[response_pd['data'] == transaction, "Fee Coin"] = 'SOL'
            response_pd.loc[response_pd['data'] == transaction, "Tag"] = 'Reward'
        elif any([True if x in scam else False for x in [x['pubkey'] for x in transaction['transaction']["message"]['accountKeys']]]):
            response_pd=response_pd[response_pd['data'] != transaction]
        elif 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp' in ','.join(transaction["meta"]["logMessages"]) and 'Instruction: Buy' in ','.join(transaction["meta"]["logMessages"]):
            response_pd.loc[response_pd['data'] == transaction, "From"] = address
            response_pd.loc[response_pd['data'] == transaction, "To"] = transaction['transaction']["message"]['accountKeys'][-1]['pubkey']
            response_pd.loc[response_pd['data'] == transaction, "From Amount"] = -sum([x['parsed']['info']['lamports'] for x in transaction['meta']['innerInstructions'][0]['instructions'] if x['programId'] == '11111111111111111111111111111111'])/10**9
            response_pd.loc[response_pd['data'] == transaction, "From Coin"] = 'SOL'
            response_pd.loc[response_pd['data'] == transaction, "To Amount"] = 1
            response_pd.loc[response_pd['data'] == transaction, "To Coin"] = [x['accounts'][0] for x in transaction['meta']['innerInstructions'][0]['instructions'] if x['programId'] == 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp'][0]
            response_pd.loc[response_pd['data'] == transaction, "Fee"] = -transaction["meta"]["fee"] / 10 ** 9
            response_pd.loc[response_pd['data'] == transaction, "Fee Coin"] = 'SOL'
            response_pd.loc[response_pd['data'] == transaction, "Notes"] = 'Buy NFT tensor'
            nfts.append([x['accounts'][0] for x in transaction['meta']['innerInstructions'][0]['instructions'] if x['programId'] == 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp'][0])
        elif 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc' in ','.join(transaction["meta"]["logMessages"]) :
            # Orca swaps
            from_amount = [x for x in transaction["meta"]["preTokenBalances"] if x['owner'] == address]
            to_amount = [x for x in transaction["meta"]["postTokenBalances"] if x['owner'] == address]
            if len(from_amount) == 0:
                response_pd.loc[response_pd['data'] == transaction, "From Amount"] = -int(transaction["meta"]["innerInstructions"][-1]['instructions'][0]['parsed']['info']['amount']) / 10**9
                response_pd.loc[response_pd['data'] == transaction, "From Coin"] = 'SOL'
            else:
                response_pd.loc[response_pd['data'] == transaction, "From Amount"] = -from_amount[-1]['uiTokenAmount']['uiAmount']
                response_pd.loc[response_pd['data'] == transaction, "From Coin"] = requests.get(f"https://api.solana.fm/v0/tokens/{from_amount[-1]['mint']}").json()['result']['data']['symbol']

            if len(to_amount) == 0:
                response_pd.loc[response_pd['data'] == transaction, "To Amount"] = -int(transaction["meta"]["innerInstructions"][-1]['instructions'][-1]['parsed']['info']['amount']) / 10 ** 9
                response_pd.loc[response_pd['data'] == transaction, "To Coin"] = 'SOL'
            else:
                response_pd.loc[response_pd['data'] == transaction, "To Amount"] = to_amount[-1]['uiTokenAmount']['uiAmount']
                response_pd.loc[response_pd['data'] == transaction, "To Coin"] = requests.get(f"https://api.solana.fm/v0/tokens/{to_amount[-1]['mint']}").json()['result']['data']['symbol']
            response_pd.loc[response_pd['data'] == transaction, "Fee"] = -transaction["meta"]["fee"] / 10 ** 9
            response_pd.loc[response_pd['data'] == transaction, "Fee Coin"] = 'SOL'
            response_pd.loc[response_pd['data'] == transaction, "Notes"] = 'Orca Swap'

        elif "'type': 'transfer'}}" in str(transaction) and "Program ComputeBudget111111111111111111111111111111 invoke"  in str(transaction):
            if transaction["transaction"]["message"]["instructions"][-1]['parsed']['info']['source'] == address:
                print('x')
                response_pd.loc[response_pd['data'] == transaction, "From"] = address
                response_pd.loc[response_pd['data'] == transaction, "From Amount"] = -transaction["transaction"]["message"]["instructions"][-1]['parsed']['info']['lamports'] / 10 ** 9
                response_pd.loc[response_pd['data'] == transaction, "From Coin"] = 'SOL'
                response_pd.loc[response_pd['data'] == transaction, "Fee"] = -transaction["meta"]["fee"] / 10 ** 9
                response_pd.loc[response_pd['data'] == transaction, "Fee Coin"] = 'SOL'
            else:
                response_pd.loc[response_pd['data'] == transaction, "From"] = transaction["transaction"]["message"]["instructions"][-1]['parsed']['info']['source']

            if transaction["transaction"]["message"]["instructions"][-1]['parsed']['info']['destination'] == address:
                print('x')
                response_pd.loc[response_pd['data'] == transaction, "To"] = address
                response_pd.loc[response_pd['data'] == transaction, "To Amount"] = transaction["transaction"]["message"]["instructions"][-1]['parsed']['info']['lamports'] / 10 ** 9
                response_pd.loc[response_pd['data'] == transaction, "To Coin"] = 'SOL'
            else:
                response_pd.loc[response_pd['data'] == transaction, "To"] = transaction["transaction"]["message"]["instructions"][-1]['parsed']['info']['destination']

            response_pd.loc[response_pd['data'] == transaction, "Notes"] = 'Sol transfer'


    return response_pd