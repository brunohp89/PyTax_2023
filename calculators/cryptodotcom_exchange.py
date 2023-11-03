# Esportare depositi, ORDERS, supercharge, withdrawal in CSV. Rimuovere le prime due righe e cancellare file vuoti
# PROBLEMI: Non vengono gestiti casi di duplicati per ora
# PROBLEMI: Se non ci sono tutti: WITHDRAWALS; DEPOSITS; SPOT_ORDER potrebbero mancare delle colonne
import os
import numpy as np
import pandas as pd
import tax_library as tx
from PricesClass import Prices


def get_transactions_df(raw=False):
    cdc_files = [
        os.path.join(os.path.abspath("crypto.com exchange"), x)
        for x in os.listdir(os.path.abspath("crypto.com exchange"))
    ]

    if len(cdc_files) == 0:
        print("No files for crypto.com exchange found")
        return None
    else:
        df_list = []
        for filename in cdc_files:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)

        if raw:
            return final_df

        final_df = final_df[final_df["Status"] != "CANCELED"].copy()

        final_df["Tag"] = ""
        final_df["Notes"] = ""
        final_df["Withdrawal Amount"] *= -1

        final_df.loc[~pd.isna(final_df["Amount"]), "Tag"] = "Interest"

        # Deposits and withdrawals

        dep_with = final_df[
            np.logical_or(
                ~pd.isna(final_df["Deposit Address"]),
                ~pd.isna(final_df["Withdrawal Address"]),
            )
        ].copy()
        dep_with["From Coin"] = dep_with["Coin"]
        dep_with = dep_with.rename(
            columns={
                "Coin": "To Coin",
                "Deposit Amount": "To Amount",
                "Withdrawal Amount": "From Amount",
                "Withdrawal Address": "To",
                "Deposit Address": "From",
                "Fee Currency": "Fee Coin",
            }
        )
        dep_with.loc[pd.isna(dep_with["To Amount"]), "From Coin"] = dep_with.loc[
            pd.isna(dep_with["To Amount"]), "To Coin"
        ]
        dep_with.loc[pd.isna(dep_with["To Amount"]), "To Coin"] = None

        dep_with.drop(
            [
                "Status",
                "TxId",
                "Order ID",
                "Trade ID",
                "Symbol",
                "Side",
                "Trade Price",
                "Trade Amount",
                "Volume of Business",
                "Amount",
                "Txid",
            ],
            inplace=True,
            axis=1,
        )

        trades = final_df[
            np.logical_and(
                pd.isna(final_df["Deposit Address"]),
                pd.isna(final_df["Withdrawal Address"]),
            )
        ].copy()
        trades.loc[~pd.isna(trades["Symbol"]), "Coin"] = trades.loc[
            ~pd.isna(trades["Symbol"]), "Symbol"
        ].map(lambda x: x.split("_")[0])

        trades.loc[~pd.isna(trades["Symbol"]), "To Coin"] = trades.loc[
            ~pd.isna(trades["Symbol"]), "Symbol"
        ].map(lambda x: x.split("_")[1])

        trades.loc[
            np.logical_and(~pd.isna(trades["Symbol"]), trades["Side"] == "SELL"),
            "Trade Amount",
        ] *= -1

        trades.loc[
            np.logical_and(~pd.isna(trades["Symbol"]), trades["Side"] == "BUY"),
            "Volume of Business",
        ] *= -1

        trades.loc[~pd.isna(trades["Trade Amount"]), "Amount"] = trades.loc[
            ~pd.isna(trades["Trade Amount"]), "Trade Amount"
        ]
        trades.loc[~pd.isna(trades["Volume of Business"]), "To Amount"] = trades.loc[
            ~pd.isna(trades["Volume of Business"]), "Volume of Business"
        ]

        trades.drop(
            [
                "Deposit Address",
                "Deposit Amount",
                "Status",
                "TxId",
                "Trade ID",
                "Symbol",
                "Trade Price",
                "Order ID",
                "Side",
                "Trade Amount",
                "Volume of Business",
                "Withdrawal Amount",
                "Withdrawal Address",
                "Txid",
            ],
            axis=1,
            inplace=True,
        )

        trades = trades.rename(
            columns={
                "Coin": "From Coin",
                "Amount": "From Amount",
                "Fee Currency": "Fee Coin",
            }
        )

        to_coin = trades.loc[trades["To Amount"] < 0, ["From Amount", "From Coin"]]
        to_coin.columns = ["To Amount", "To Coin"]
        from_coin = trades.loc[trades["To Amount"] < 0, ["To Amount", "To Coin"]]
        from_coin.columns = ["From Amount", "From Coin"]

        trades.loc[trades["To Amount"] < 0, "Notes"] = "Fixed"

        trades.loc[trades["Notes"] == "Fixed", ["To Amount", "To Coin"]] = to_coin
        trades.loc[trades["Notes"] == "Fixed", ["From Amount", "From Coin"]] = from_coin
        trades.loc[trades["Notes"] == "Fixed", "Notes"] = ""
        trades["From"], trades["To"] = (None, None)

        trades.drop_duplicates(
            inplace=True,
            subset=[
                "Time (UTC)",
                "From Coin",
                "From Amount",
                "From",
                "To",
                "To Coin",
                "To Amount",
            ],
        )

        vout = pd.concat([trades, dep_with])

        vout.index = [tx.str_to_datetime(j[:-4]) for j in vout["Time (UTC)"]]
        vout.drop(["Time (UTC)"], axis=1, inplace=True)

        vout.loc[vout["Tag"] == "", "Tag"] = "Movement"
        vout["Fiat"] = "EUR"
        vout["Fiat Price"] = None
        vout["Fee Fiat"] = None

        vout.sort_index(inplace=True)

        vout.loc[vout["From Coin"] == vout["To Coin"], "From Coin"] = None

        exch_prices = Prices()
        vout = tx.price_transactions_df(vout, exch_prices)

        vout["Source"] = "Crypto.com Exchange"
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

        vout["Fee"] *= -1

        vout['Fiat Price'] = [abs(k) if (~pd.isna(k) and k is not None) else k for k in vout['Fiat Price']]
        vout['Fee'] = [-abs(k) if (~pd.isna(k) and k is not None) else k for k in vout['Fee']]
        vout['Fee Fiat'] = [-abs(k) if (~pd.isna(k) and k is not None) else k for k in vout['Fee Fiat']]

        toswitch = vout[vout['To Amount'] < 0]
        if toswitch.shape[0] > 0:
            toswitch = toswitch.rename(
                columns={'To Amount': 'From Amount', 'To Coin': 'From Coin', 'From Amount': 'To Amount',
                         'From Coin': 'To Coin'})

            vout = pd.concat([vout[np.logical_or(vout['To Amount'] > 0, pd.isna(vout['To Amount']))], toswitch])

        toswitch = vout[vout['From Amount'] > 0]
        if toswitch.shape[0] > 0:
            toswitch = toswitch.rename(
                columns={'From Amount': 'To Amount', 'From Coin': 'To Coin', 'To Amount': 'From Amount',
                         'To Coin': 'From Coin'})

            vout = pd.concat([vout[np.logical_or(vout['From Amount'] < 0, pd.isna(vout['From Amount']))], toswitch])
        vout = vout.sort_index()
        return vout


def get_eur_invested(year=None):
    all_trx = get_transactions_df()
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    return -all_trx.loc[all_trx["From Coin"] == "EUR", "From Amount"].sum()
