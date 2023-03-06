import requests
import datetime as dt
import pandas as pd
import tax_library as tx
from PricesClass import Prices


def get_transactions_df(address, bridged_assets=None):
    address = address.lower()
    response = requests.get(f"https://stepscan.io/api?module=account&action=txlist&address={address}")
    response = pd.DataFrame(response.json()['result'])

    response['Fee'] = (int(response['gasUsed']) * int(response['gasPrice'])) / 10 ** 18
    response['From Amount'] = (int(response['gasUsed']) * int(response['gasPrice'])) / 10 ** 18

    response['value'] = int(response['value']) / 10 ** 18

    response['From Amount'] = None
    response['To Amount'] = None
    response['From Coin'] = None
    response['To Coin'] = None
    response['Fee Fiat'] = None
    response['Fee Coin'] = 'FITFI'
    response['Fiat'] = 'EUR'
    response['Fiat Price'] = None
    response['Notes'] = ''
    response['Tag'] = 'Movement'
    response['Source'] = f'StepApp-{address[0:5]}'

    response['from'] = [k.lower() for k in response['from']]
    response['to'] = [k.lower() for k in response['to']]
    response.loc[response['from'] == address, 'From Amount'] = -response.loc[response['from'] == address, 'value']
    response.loc[response['to'] == address, 'To Amount'] = response.loc[response['to'] == address, 'value']

    response.loc[response['from'] == address, 'From Coin'] = 'FITFI'
    response.loc[response['to'] == address, 'To Amount'] = 'FITFI'

    response.index = [dt.datetime.fromtimestamp(int(x)) for x in response["timeStamp"]]

    response.rename(
        columns={
            "to": "To",
            "from": "From"
        },
        inplace=True,
    )

    response.drop(['blockHash', 'blockNumber', 'confirmations', 'contractAddress',
                   'cumulativeGasUsed', 'gas', 'gasPrice', 'gasUsed', 'hash',
                   'input', 'isError', 'nonce', 'timeStamp', 'transactionIndex',
                   'txreceipt_status', 'value'], axis=1, inplace=True)

    response = response[
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

    if bridged_assets is not None:
        for asset in bridged_assets.keys():
            ind = min(response.index)
            temp_df = response.iloc[[0], :].copy()
            temp_df["To Coin"] = asset
            temp_df["To Amount"] = bridged_assets[asset]
            temp_df["Fee"] = temp_df["Fee Coin"] = temp_df["Fee Fiat"] = temp_df[
                "From Amount"
            ] = temp_df["From Coin"] = temp_df["Notes"] = None
            temp_df["Tag"] = "Bridged to Step Chain"
            temp_df.index = [ind]
            response = pd.concat([temp_df,response])
        response.sort_index(inplace=True)


    response = tx.price_transactions_df(response, Prices())

    return response
