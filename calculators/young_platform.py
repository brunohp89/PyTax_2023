import os
import numpy as np
import pandas as pd
from PricesClass import Prices
import tax_library as tx
import datetime as dt


# Extract only movements history, no need to extract orders as the orders are already included in the movements


def get_transactions_df(raw=False):
    transactions_yp = [
        os.path.join(os.path.abspath("input/young_platform"), x)
        for x in os.listdir(os.path.abspath("input/young_platform"))
    ]
    if len(transactions_yp) == 0:
        print("No files for young platform found")
        return None
    else:
        df_list = []
        for filename in transactions_yp:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)
        final_df.drop_duplicates(inplace=True)
        final_df.reset_index(inplace=True, drop=True)
        if raw:
            return final_df

        # YNG will be considered only when exchanged by Insta trade to EUR
        final_df = final_df.query("currency != 'YNG'")

        final_df["debit"] *= -1
        final_df["date"] = final_df["date"].apply(lambda x: x.split(".")[0])
        final_df["From Coin"] = None
        final_df["Fee"] = None
        final_df["Fee Fiat"] = None
        final_df["Fee Coin"] = None
        final_df["Fiat"] = None
        final_df["Fiat Price"] = None
        final_df["Notes"] = ""
        final_df["Source"] = "Young Platform"
        final_df["From"] = None
        final_df["To"] = None

        for i in final_df.query("tx_type == 'ORDER_PLACEMENT'")["id"]:
            temp_df = final_df[final_df["id"].isin([i, i + 1])].copy()
            if "ORDER_CANCELLED" in temp_df["tx_type"].tolist():
                temp_df = final_df[final_df["id"].isin([i, i + 2])].copy()
                final_df = final_df.query(f"id not in ({i},{i + 2})")
            else:
                final_df = final_df.query(f"id not in ({i},{i + 1})")

            temp_df.loc[temp_df["debit"] == 0, "From Coin"] = temp_df.loc[
                temp_df["debit"] < 0, "currency"
            ].values[0]
            temp_df.loc[temp_df["debit"] == 0, "debit"] = temp_df.loc[
                temp_df["debit"] < 0, "debit"
            ].values[0]

            final_df = pd.concat(
                [final_df, temp_df.loc[~pd.isna(temp_df["From Coin"]), :]], axis=0
            )

        for i in final_df.query("tx_type == 'WITHDRAWAL'")["id"]:
            temp_df = final_df[final_df["id"].isin([i, i + 1])].copy()
            final_df = final_df.query(f"id not in ({i},{i + 1})")

            temp_df.loc[temp_df["tx_type"] == "WITHDRAWAL", "Fee Coin"] = temp_df.loc[
                temp_df["tx_type"] == "FEE", "currency"
            ].values[0]
            temp_df.loc[temp_df["tx_type"] == "WITHDRAWAL", "Fee"] = temp_df.loc[
                temp_df["tx_type"] == "FEE", "debit"
            ].values[0]

            temp_df["From Coin"] = temp_df["currency"].tolist()
            temp_df["currency"] = None

            final_df = pd.concat(
                [final_df, temp_df.loc[~pd.isna(temp_df["Fee Coin"]), :]], axis=0
            )  # pd.concat([final_df, temp_df.iloc[[0], :]], axis=0)

        final_df.loc[final_df["tx_type"] == "INSTA_TRADE", "tx_type"] = "Reward"
        final_df.loc[
            final_df["tx_type"].str.contains("ORDER"), "tx_type"
        ] = "Trade"  # 'INSTA_TRADE', 'tx_type'] = 'Reward'
        final_df.loc[
            final_df["tx_type"].isin(["WITHDRAWAL", "DEPOSIT"]), "tx_type"
        ] = "Movement"

        final_df.index = [
            dt.datetime.strptime(k, "%Y-%m-%dT%H:%M:%S") for k in final_df["date"]
        ]

        final_df = final_df.drop(["id", "date"], axis=1)

        final_df = final_df.rename(
            columns={
                "credit": "To Amount",
                "debit": "From Amount",
                "currency": "To Coin",
                "tx_type": "Tag",
            }
        )

        vout = tx.price_transactions_df(final_df, Prices())
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

        vout['Fiat Price'] = [abs(k) if (~pd.isna(k) and k is not None) else k for k in vout['Fiat Price']]
        vout['Fee'] = [-abs(k) if (~pd.isna(k) and k is not None) else k for k in vout['Fee']]
        vout['Fee Fiat'] = [-abs(k) if (~pd.isna(k) and k is not None) else k for k in vout['Fee Fiat']]

        return vout


def get_eur_invested(year=None):
    all_trx = get_transactions_df()
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    all_trx = all_trx[
        np.logical_and(all_trx["To Coin"] == "EUR", pd.isna(all_trx["From Coin"]))
    ]
    all_trx = all_trx[all_trx["Tag"] == "Movement"]
    return all_trx["To Amount"].sum()
