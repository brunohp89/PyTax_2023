import os
import datetime as dt
import numpy as np
import pandas as pd
import tax_library as tx
from PricesClass import Prices


def get_transactions_df(raw=False):
    vechain_directory = os.path.abspath('vechain')
    vechain_files = [
        os.path.join(vechain_directory, x) for x in os.listdir(vechain_directory)
    ]

    if not vechain_files:
        print("No files for VeChain found")
        return None

    df_list = [
        pd.read_csv(filename, index_col=None, header=0) for filename in vechain_files
    ]
    final_df = pd.concat(df_list, axis=0, ignore_index=True)
    final_df.index = [tx.str_to_datetime(x) for x in final_df["Date(GMT)"]]

    final_df.drop_duplicates(inplace=True, subset=["Txid"])
    final_df.sort_index(inplace=True)

    cols_to_add = [
        "Fee",
        "Source",
        "Notes",
        "Fee Fiat",
        "Fee Coin",
        "To Coin",
        "To Amount",
        "Fiat",
        "Fiat Price",
    ]
    values_to_add = [
        None,
        f'VET-{vechain_files[0].split("export-")[1][0:5]}',
        "",
        None,
        "VTHO",
        None,
        None,
        "EUR",
        0,
    ]

    for col, value in zip(cols_to_add, values_to_add):
        final_df[col] = value

    final_df.rename(
        columns={
            "Remark": "Tag",
            "Sender": "From",
            "Recipient": "To",
            "Token": "From Coin",
            "Amount": "From Amount",
        },
        inplace=True,
    )

    final_df.drop(["Txid", "Block# ", "Date(GMT)"], axis=1, inplace=True)

    final_df["Tag"] = "Movement"

    if final_df["From Amount"].sum() < 1:
        vtho_transactions = pd.date_range(
            min(final_df.index), max(final_df.index), freq="d"
        )
    else:
        vtho_transactions = pd.date_range(
            min(final_df.index), dt.datetime.now() - dt.timedelta(days=1), freq="d"
        )

    vtho_df = pd.DataFrame(
        index=vtho_transactions,
        data=np.zeros([len(vtho_transactions), final_df.shape[1]]),
        columns=final_df.columns,
    )
    vtho_df.index = [tx.str_to_datetime(str(x)) for x in vtho_df.index]
    vtho_date = [x.date() for x in vtho_df.index]

    vet_bal = tx.balances(final_df)

    vtho_df = pd.DataFrame(
        {
            "From Amount": [
                v * 0.000432 for v in vet_bal.loc[vtho_date, "VET"] if v >= 1
            ],
            "Fee": None,
            "To": None,
            "From": None,
            "Fee Coin": "VTHO",
            "From Coin": "VTHO",
            "To Coin": None,
            "To Amount": None,
            "Fiat": "EUR",
            "Fiat Price": 0,
            "Tag": "Reward",
            "Fee Fiat": None,
            "Notes": "",
            "Source": f'VET-{vechain_files[0].split("export-")[1][0:5]}',
        },
        index=[dt.datetime.combine(x, dt.time(0, 0, 0)) for x in vtho_date[:-1]],
    )

    final_df = pd.concat([final_df, vtho_df], axis=0)

    if raw:
        return final_df

    final_df = tx.price_transactions_df(final_df, Prices())

    final_df = final_df[
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

    return final_df
