import os
import numpy as np
import pandas as pd
from PricesClass import Prices
import tax_library as tx


def get_transactions_df(raw=False):
    transactions_uphold = [
        os.path.join(os.path.abspath("uphold"), x)
        for x in os.listdir(os.path.abspath("uphold"))
    ]
    if len(transactions_uphold) == 0:
        print("No files for uphold found")
        return None
    else:
        df_list = []
        for filename in transactions_uphold:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)
        final_df.reset_index(inplace=True, drop=True)
        final_df = final_df.drop_duplicates()
        if raw:
            return final_df
        for i in range(final_df.shape[0]):
            if final_df.loc[i, "Destination"] != "uphold":
                final_df.loc[i, "Destination Amount"] *= -1
                if not pd.isna(final_df.loc[i, "Fee Amount"]):
                    final_df.loc[i, "Destination Amount"] += (
                            final_df.loc[i, "Fee Amount"] * -1
                    )
            if (
                    final_df.loc[i, "Origin Currency"]
                    == final_df.loc[i, "Destination Currency"]
            ):
                final_df.loc[i, "Origin Amount"] = 0
            else:
                final_df.loc[i, "Origin Amount"] *= -1

        final_df.index = [
            tx.uphold_date_to_datetime(final_df.loc[i, "Date"])
            for i in range(final_df.shape[0])
        ]

        final_df.sort_index(inplace=True)
        final_df["From"] = None
        final_df["To"] = None
        final_df["Fiat Price"] = None
        final_df["Fiat"] = "EUR"

        final_df.rename(
            columns={
                "Origin Currency": "From Coin",
                "Origin Amount": "From Amount",
                "Destination Currency": "To Coin",
                "Destination Amount": "To Amount",
                "Fee Amount": "Fee",
                "Type": "Tag",
                "Fee Currency": "Fee Coin",
            },
            inplace=True,
        )

        final_df.loc[final_df["Tag"] == "out", "From Amount"] = final_df.loc[
            final_df["Tag"] == "out", "To Amount"
        ]
        final_df.loc[final_df["Tag"] == "out", "To Amount"] = None
        final_df.loc[final_df["Tag"] == "out", "To Coin"] = None

        final_df.drop(
            ["Date", "Destination", "Id", "Origin", "Status"], axis=1, inplace=True
        )
        final_df["Fee"].fillna("", inplace=True)
        final_df["Fee Coin"].fillna("", inplace=True)
        final_df.fillna(0, inplace=True)
        final_df["Fee Fiat"] = None
        final_df["Source"] = "Uphold"
        final_df["Notes"] = ""

        final_df.loc[
            np.logical_and(
                final_df["From Coin"] == "BAT", final_df["From Amount"] == 0
            ),
            "From Amount",
        ] = final_df.loc[
            np.logical_and(
                final_df["From Coin"] == "BAT", final_df["From Amount"] == 0
            ),
            "To Amount",
        ]

        final_df.loc[final_df["Fiat Price"] == 0, "Fiat Price"] = None

        final_df.loc[final_df['From Coin'] == 0, 'From Coin'] = None
        final_df.loc[final_df['To Coin'] == 0, 'To Coin'] = None

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

        final_df["Tag"] = "Movement"
        final_df.loc[
            np.logical_and(final_df["From Coin"] == "BAT", final_df["From Amount"] > 0),
            "Tag",
        ] = "Reward"
        final_df.loc[
            np.logical_and(final_df["From Coin"] == "BAT", final_df["From Amount"] > 0),
            "From Amount",
        ] = None
        final_df.loc[
            np.logical_and(
                final_df["From Coin"] == "BAT", pd.isna(final_df["From Amount"])
            ),
            "From Coin",
        ] = None
        final_df.loc[
            np.logical_and(pd.isna(final_df["From"]), final_df["From Amount"] < 0), "From"
        ] = "Uphold"

        final_df["Fee"] = [0 if k is not None else None for k in final_df["Fee"]]

        final_df.loc[final_df['Fee'] == "", 'Fee'] = None
        final_df.loc[final_df['From Coin'] == "", 'From Coin'] = None
        final_df.loc[final_df['From Amount'] == "", 'From Amount'] = None
        final_df.loc[final_df['To Coin'] == "", 'To Coin'] = None
        final_df.loc[final_df['To Amount'] == "", 'To Amount'] = None

        final_df['Fiat Price'] = [abs(k) if (~pd.isna(k) and k is not None) else k for k in final_df['Fiat Price']]
        final_df['Fee Fiat'] = [-abs(k) if (~pd.isna(k) and k is not None) else k for k in final_df['Fee Fiat']]

        return final_df


def get_eur_invested(year=None):
    all_trx = get_transactions_df(raw=True)
    all_trx.index = [
        tx.uphold_date_to_datetime(all_trx.loc[i, "Date"])
        for i in range(all_trx.shape[0])
    ]
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    return all_trx.loc[all_trx["Origin Currency"] == "EUR", "Origin Amount"].sum()
