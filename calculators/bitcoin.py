import numpy as np
from PricesClass import Prices
import pandas as pd
import requests
import tax_library as tx


def get_transactions_df(address_list):
    print('REMINDER: If the total value of your UTXOs is greater than the amount you want to send, the excess is sent '
          'back to a new address in your own wallet. This new address is known as the change address. Include any '
          'change wallet to the address_list')

    vout = pd.DataFrame()
    for address in address_list:
        url = f"https://api.blockchair.com/bitcoin/dashboards/address/{address}?transaction_details=true"
        response = requests.get(url)
        if vout.shape[0] == 0:
            vout = pd.DataFrame(response.json()["data"][address]["transactions"])
        else:
            vout = pd.concat(
                [pd.DataFrame(response.json()["data"][address]["transactions"]), vout],
                axis=0,
            )

    vout.index = vout["time"].map(lambda x: tx.str_to_datetime(x))

    vout["balance_change"] /= 10**8

    vout.rename(columns={"balance_change": "From Amount"}, inplace=True)
    vout.drop(["block_id", "hash", "time"], axis=1, inplace=True)

    vout["Fee"] = None
    vout["Fee Currency"] = "BTC"
    vout["Fiat Price"] = None
    vout["Fiat"] = "EUR"
    vout["Fee Coin"] = "BTC"
    vout["To Coin"] = None
    vout["From Coin"] = "BTC"
    vout["To Amount"] = None
    vout["Tag"] = "Movement"
    vout["From"] = None
    vout["To"] = ",".join(address_list)
    vout["Fee Fiat"] = None
    vout["Notes"] = None
    vout["Source"] = "BTC"

    vout.sort_index(inplace=True)
    vout["From Amount"].fillna(0, inplace=True)
    vout["From"].fillna("", inplace=True)
    vout["To"].fillna("", inplace=True)

    vout.loc[vout["From"].isin(address_list), "From Amount"] *= -1

    vout = vout[
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

    vout = tx.price_transactions_df(vout, Prices())

    vout.loc[vout['From Amount'] > 0, 'To Amount'] = vout.loc[vout['From Amount'] > 0, 'From Amount']
    vout.loc[vout['From Amount'] > 0, 'To Coin'] = vout.loc[vout['From Amount'] > 0, 'From Coin']
    vout.loc[vout['From Amount'] > 0, ['From Amount', 'From Coin']] = None

    return vout
