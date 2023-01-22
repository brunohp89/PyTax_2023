# WITHDRAWAL FEES non sono incluse nel file, aggiungere a mano
import os
import pandas as pd
import tax_library as tx
from PricesClass import Prices
import datetime as dt


def get_transactions_df():
    orders = [
        os.path.join(os.getcwd(), "kucoin", x)
        for x in os.listdir(os.path.join(os.getcwd(), "kucoin"))
        if "Ordini" in x
    ]

    if len(orders) == 0:
        print("No orders for kucoin found")
    else:
        df_list = []
        for filename in orders:
            df_loop = pd.read_excel(filename, engine="openpyxl")
            df_list.append(df_loop)
        orders_df = pd.concat(df_list, axis=0, ignore_index=True)
        orders_df.drop_duplicates(inplace=True)
        orders_df = orders_df[orders_df["Maker/Taker"] != "TAKER"]

        orders_df.index = [
            tx.str_to_datetime(j) - dt.timedelta(hours=8)
            for j in orders_df["Filled Time(UTC+08:00)"]
        ]

        orders_df["From"] = None
        orders_df["To"] = None
        orders_df["Filled Volume (USDT)"] *= -1
        orders_df["From Coin"] = [k.split("-")[1] for k in orders_df["Symbol"]]
        orders_df["To Coin"] = [k.split("-")[0] for k in orders_df["Symbol"]]
        orders_df["Fiat Price"] = (
            orders_df["Avg. Filled Price"] * orders_df["Filled Amount"]
        )
        orders_df["Fiat"] = "EUR"
        orders_df["Notes"] = ""
        orders_df["Tag"] = "Trade"
        orders_df["Source"] = "Kucoin"
        orders_df["Fee Fiat"] = None

        orders_df.drop(
            [
                "Account Type",
                "Order ID",
                "Symbol",
                "Order Type",
                "Avg. Filled Price",
                "Filled Volume",
                "Order Time(UTC+08:00)",
                "Filled Time(UTC+08:00)",
                "Maker/Taker",
                "Order Price",
                "Order Amount",
                "Status",
                "Filled Price",
                "Filled Value",
                "Filled Value (USDT)",
                "Fee Rate",
                "UID",
                "Side",
            ],
            axis=1,
            inplace=True,
        )

        orders_df.rename(
            columns={
                "Filled Amount": "To Amount",
                "Filled Volume (USDT)": "From Amount",
                "Fee Currency": "Fee Coin",
            },
            inplace=True,
        )

    deposits = [
        os.path.join(os.getcwd(), "kucoin", x)
        for x in os.listdir(os.path.join(os.getcwd(), "kucoin"))
        if "deposit" in x
    ]

    if len(deposits) == 0:
        print("No deposits for kucoin found")
    else:
        df_list = []
        for filename in deposits:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        deposits_df = pd.concat(df_list, axis=0, ignore_index=True)
        deposits_df.drop_duplicates(inplace=True)

        deposits_df.index = [
            tx.str_to_datetime(j) - dt.timedelta(hours=8) for j in deposits_df["Time"]
        ]

        deposits_df["From"] = deposits_df["Network"]
        deposits_df["To"] = None
        deposits_df["Fee Coin"] = None
        deposits_df["Fee"] = None
        deposits_df["From Amount"] = None
        deposits_df["From Coin"] = None
        deposits_df["To Coin"] = deposits_df["Coin"]
        deposits_df["To Amount"] = deposits_df["Amount"]
        deposits_df["Fiat Price"] = None
        deposits_df["Fiat"] = "EUR"
        deposits_df["Notes"] = ""
        deposits_df["Tag"] = "Deposit"
        deposits_df["Source"] = "Kucoin"
        deposits_df["Fee Fiat"] = None

        deposits_df.drop(
            ["Time", "Coin", "Network", "Amount", "Type", "Remark"],
            inplace=True,
            axis=1,
        )

    withdraws = [
        os.path.join(os.getcwd(), "kucoin", x)
        for x in os.listdir(os.path.join(os.getcwd(), "kucoin"))
        if "withdrawal" in x
    ]

    if len(withdraws) == 0:
        print("No withdrawals for kucoin found")
    else:
        df_list = []
        for filename in withdraws:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        withdraw_df = pd.concat(df_list, axis=0, ignore_index=True)
        withdraw_df.drop_duplicates(inplace=True)

        withdraw_df.index = [
            tx.str_to_datetime(j) - dt.timedelta(hours=8) for j in withdraw_df["Time"]
        ]

        withdraw_df["From"] = None
        withdraw_df["To"] = withdraw_df["Wallet Address/Account"]
        withdraw_df["From Amount"] = -withdraw_df["Amount"]
        withdraw_df["From Coin"] = withdraw_df["Coin"]
        withdraw_df["To Coin"] = None
        withdraw_df["To Amount"] = None
        withdraw_df["Fiat Price"] = None
        withdraw_df["Fiat"] = "EUR"
        withdraw_df["Notes"] = ""
        withdraw_df["Tag"] = "Withdraw"
        withdraw_df["Source"] = "Kucoin"
        withdraw_df["Fee Fiat"] = None

        withdraw_df.drop(
            [
                "Wallet Address/Account",
                "Time",
                "Coin",
                "Network",
                "Amount",
                "Type",
                "Remark",
            ],
            inplace=True,
            axis=1,
        )

    convert = [
        os.path.join(os.getcwd(), "kucoin", x)
        for x in os.listdir(os.path.join(os.getcwd(), "kucoin"))
        if "convert" in x
    ]

    if len(convert) == 0:
        print("No withdrawals for kucoin found")
    else:
        df_list = []
        for filename in convert:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        convert_df = pd.concat(df_list, axis=0, ignore_index=True)
        convert_df.drop_duplicates(inplace=True)

        convert_df.index = [
            tx.str_to_datetime(j.replace("/", "-")) - dt.timedelta(hours=8)
            for j in convert_df["Time Filled"]
        ]

        convert_df["From"] = None
        convert_df["Fee Coin"] = None
        convert_df["Fee"] = None
        convert_df["From Amount"] = [
            -float(x.split(" ")[0]) + float(y.split(" ")[0])
            for x, y in zip(
                convert_df["From(Main Account)"], convert_df["From(Trading Account)"]
            )
        ]
        convert_df["From Coin"] = [
            x.split(" ")[1] for x in convert_df["From(Main Account)"]
        ]
        convert_df["To Coin"] = [x.split(" ")[1] for x in convert_df["To"]]
        convert_df["To Amount"] = [float(x.split(" ")[0]) for x in convert_df["To"]]
        convert_df["Fiat Price"] = None
        convert_df["Fiat"] = "EUR"
        convert_df["Notes"] = ""
        convert_df["To"] = None
        convert_df["Tag"] = "Trade"
        convert_df["Source"] = "Kucoin"
        convert_df["Fee Fiat"] = None

        convert_df.drop(
            [
                "Status",
                "From(Trading Account)",
                "From(Main Account)",
                "Time Filled",
                "Paid by",
            ],
            inplace=True,
            axis=1,
        )

        final_df = pd.concat([withdraw_df, convert_df, orders_df, deposits_df])
        final_df.sort_index(inplace=True)

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


def get_eur_invested(year=None):
    all_trx = get_transactions_df()
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    return all_trx.loc[all_trx["From Coin"] == "EUR", "From Amount"].sum()
