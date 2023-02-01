import numpy as np

from PricesClass import Prices
import datetime as dt
import pandas as pd
import requests
import tax_library as tx
import os
import pickle as pk


def get_transaction_contents(tokens_transactions):
    contents_pd = pd.DataFrame()
    contents, hashes = [], []
    for transaction in set(tokens_transactions["Txhash"]):
        resp_loop = requests.get(
            f"https://public-api.solscan.io/transaction/{transaction}"
        )
        i = 0
        while i <= 3 and resp_loop.status_code != 200:
            resp_loop = requests.get(
                f"https://public-api.solscan.io/transaction/{transaction}"
            )
        contents.append(resp_loop.json())
        hashes.append(transaction)
    contents_pd["Txhash"] = hashes
    contents_pd["Txcontent"] = contents

    return contents_pd


def get_transactions_df(address):
    os.makedirs(f"{os.getcwd()}\\solana", exist_ok=True)

    response = requests.get(
        f"https://public-api.solscan.io/account/exportTransactions?account={address}&type=all&fromTime=1611839871&toTime={int(dt.datetime.now().timestamp())}"
    )
    with open("temp.csv", "wb") as handle:
        handle.write(response.content)
    response_pd = pd.read_csv("temp.csv")
    response_pd.columns = [k.strip() for k in response_pd.columns]
    os.remove("temp.csv")

    # Current staking - WIP
    # response = requests.get(f'https://public-api.solscan.io/account/stakeAccounts?account={address}')

    normal_transactions = response_pd[response_pd["Symbol(off-chain)"] == "SOL"].copy()
    normal_transactions["From"] = normal_transactions["SolTransfer Source"]
    normal_transactions["To"] = normal_transactions["SolTransfer Destination"]
    normal_transactions["From Amount"] = normal_transactions["Amount (SOL)"]
    normal_transactions["From Coin"] = "SOL"
    normal_transactions["To Amount"] = None
    normal_transactions["To Coin"] = None
    normal_transactions["Fee"] = -normal_transactions["Fee (SOL)"]
    normal_transactions["Fee Coin"] = "SOL"
    normal_transactions["Fee Fiat"] = None
    normal_transactions["Tag"] = "Movement"
    normal_transactions["Notes"] = ""
    normal_transactions["Source"] = f"Solana-{address[0:4]}"
    normal_transactions["Fiat Price"] = None
    normal_transactions["Fiat"] = "EUR"
    normal_transactions.index = [
        dt.datetime.fromtimestamp(pd.Timestamp(k).timestamp())
        for k in normal_transactions["BlockTime"]
    ]
    normal_transactions.loc[normal_transactions["From"] == address, "From Amount"] *= -1
    normal_transactions.loc[normal_transactions["To"] == address, "Fee"] *= 0

    normal_transactions.sort_index(inplace=True)
    normal_transactions.drop(
        [
            "Type",
            "Txhash",
            "BlockTime Unix",
            "BlockTime",
            "Fee (SOL)",
            "TokenAccount",
            "ChangeType",
            "SPL BalanceChange",
            "PreBalancer",
            "PostBalancer",
            "TokenAddress",
            "TokenName(off-chain)",
            "Symbol(off-chain)",
            "SolTransfer Source",
            "SolTransfer Destination",
            "Amount (SOL)",
        ],
        axis=1,
        inplace=True,
    )

    sub1 = normal_transactions.loc[
        normal_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
    ]
    sub2 = normal_transactions.loc[
        normal_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
    ]
    normal_transactions.loc[
        normal_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
    ] = sub1.values
    normal_transactions.loc[
        normal_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
    ] = sub2.values

    tokens_transactions = response_pd[response_pd["Symbol(off-chain)"] != "SOL"].copy()
    if tokens_transactions.shape[0] > 0:
        if f"{address[0:10]}-contents.pickle" in os.listdir(f"{os.getcwd()}\\solana"):
            with open(
                f"{os.getcwd()}\\solana\\{address[0:10]}-contents.pickle", "rb"
            ) as handle:
                history_df = pk.load(handle)
            tokens_transactions = pd.merge(
                tokens_transactions, history_df, on="Txhash", how="left"
            )
            new_transactions = tokens_transactions[
                pd.isna(tokens_transactions["Txcontent"])
            ]
            if new_transactions.shape[0] > 0:
                contents_pd = pd.concat(
                    [history_df, get_transaction_contents(new_transactions)]
                )
                with open(
                    f"{os.getcwd()}\\solana\\{address[0:10]}-contents.pickle", "wb"
                ) as handle:
                    pk.dump(contents_pd, handle)
                tokens_transactions = response_pd[
                    response_pd["Symbol(off-chain)"] != "SOL"
                ].copy()
                tokens_transactions = pd.merge(
                    tokens_transactions, contents_pd, on="Txhash", how="left"
                )
        else:
            contents_pd = get_transaction_contents(tokens_transactions)
            with open(
                f"{os.getcwd()}\\solana\\{address[0:10]}-contents.pickle", "wb"
            ) as handle:
                pk.dump(contents_pd, handle)
            tokens_transactions = pd.merge(
                tokens_transactions, contents_pd, on="Txhash", how="left"
            )

        tokens_transactions["Tag"] = ""
        tokens_transactions["Notes"] = ""
        tokens_transactions["From"] = ""
        tokens_transactions["To"] = ""
        tokens_transactions["From Amount"] = None
        tokens_transactions["From Coin"] = None
        tokens_transactions["To Amount"] = None
        tokens_transactions["To Coin"] = None

        # tokens_transactions.drop_duplicates(subset=['Txhash'], inplace=True)
        # tokens_transactions['Txcontent'] = [k[0] for k in tokens_transactions['Txcontent']]
        for transaction in set(tokens_transactions["Txhash"]):
            trx_content = tokens_transactions.loc[
                tokens_transactions["Txhash"] == transaction, "Txcontent"
            ].tolist()[0]
            swaps = [i for i, k in enumerate(trx_content["logMessage"]) if "Swap" in k]
            transfers = [
                i for i, k in enumerate(trx_content["logMessage"]) if "Transfer" in k
            ]
            temp_df = tokens_transactions[
                tokens_transactions["Txhash"] == transaction
            ].iloc[[0], :]
            if len(swaps) > 0:
                amount, coin = [], []
                for i in range(len(swaps)):
                    coin.extend(
                        [
                            k["extra"]["symbol"]
                            for k in trx_content["innerInstructions"][i][
                                "parsedInstructions"
                            ]
                            if "mint" not in k["name"]
                        ]
                    )
                    amount.extend(
                        [
                            k["params"]["amount"]
                            for k in trx_content["innerInstructions"][i][
                                "parsedInstructions"
                            ]
                            if "mint" not in k["name"]
                        ]
                    )
                tokens_transactions = tokens_transactions[
                    tokens_transactions["Txhash"] != transaction
                ]
                temp_df["From Coin"] = coin[0]
                temp_df["To Coin"] = coin[-1]
                temp_df["From Amount"] = -int(amount[0]) / 10**9
                temp_df["To Amount"] = int(amount[-1]) / 10**9
                temp_df["Tag"] = "Trade"
                temp_df["Notes"] = "STEPN exchange"
                tokens_transactions = pd.concat([tokens_transactions, temp_df])
            elif len(transfers) > 0:
                tokens_transactions = tokens_transactions[
                    tokens_transactions["Txhash"] != transaction
                ]
                temp_df["To"] = trx_content["tokenTransfers"][0]["destination_owner"]
                temp_df["From"] = trx_content["tokenTransfers"][0]["source_owner"]
                temp_df["To Amount"] = (
                    int(trx_content["tokenTransfers"][0]["amount"]) / 10**9
                )
                temp_df["To Coin"] = trx_content["tokenTransfers"][0]["token"]["symbol"]
                temp_df["Tag"] = "Movement"
                tokens_transactions = pd.concat([tokens_transactions, temp_df])
            elif temp_df["SPL BalanceChange"].values[0] == 0:
                continue
            else:
                print(
                    f"Attention: transaction {transaction} for Solana address {address} is not being correctly calculated"
                )
                break

        tokens_transactions["Fee"] = tokens_transactions["Fee (SOL)"]
        tokens_transactions.index = [
            dt.datetime.fromtimestamp(pd.Timestamp(k).timestamp())
            for k in tokens_transactions["BlockTime"]
        ]
        tokens_transactions.sort_index(inplace=True)
        tokens_transactions["Fee Coin"] = "SOL"
        tokens_transactions["Fee Fiat"] = None
        tokens_transactions["Source"] = f"Solana-{address[0:4]}"
        tokens_transactions["Fiat Price"] = None
        tokens_transactions["Fiat"] = "EUR"

        tokens_transactions.drop(
            [
                "Type",
                "Txhash",
                "BlockTime Unix",
                "BlockTime",
                "Fee (SOL)",
                "TokenAccount",
                "ChangeType",
                "SPL BalanceChange",
                "PreBalancer",
                "PostBalancer",
                "TokenAddress",
                "TokenName(off-chain)",
                "Symbol(off-chain)",
                "SolTransfer Source",
                "SolTransfer Destination",
                "Amount (SOL)",
                "Txcontent",
            ],
            inplace=True,
            axis=1,
        )

        tokens_transactions.loc[
            tokens_transactions["From"] == address, "To Amount"
        ] *= -1
        tokens_transactions.loc[tokens_transactions["To"] == address, "Fee"] *= 0

        sub1 = tokens_transactions.loc[
            tokens_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ]
        sub2 = tokens_transactions.loc[
            tokens_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ]
        tokens_transactions.loc[
            tokens_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ] = sub1.values
        tokens_transactions.loc[
            tokens_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ] = sub2.values
        vout = pd.concat([tokens_transactions, normal_transactions])
        vout.sort_index(inplace=True)

        vout.loc[vout["From"].str.contains("STEPN", na=False), "Notes"] = "STEPN"
        vout.loc[vout["To"].str.contains("STEPN", na=False), "Notes"] = "STEPN"

    else:
        vout = normal_transactions.copy()

    vout["Fee"] = vout["Fee"].apply(lambda x: -abs(x))

    stepn = vout[
        np.logical_or(
            vout["From"].str.contains("STEPN"), vout["To"].str.contains("STEPN")
        )
    ]
    stepn_bal = tx.balances(stepn)
    vout.loc[
        np.logical_and(
            np.logical_or(
                vout["From"].str.contains("STEPN"), vout["To"].str.contains("STEPN")
            ),
            np.logical_or(
                vout["From Coin"].isin(stepn_bal.columns),
                vout["To Coin"].isin(stepn_bal.columns),
            ),
        ),
        "Tag",
    ] = "Reward"

    # Means that we have some staking rewards or rewards from stepn
    if vout["From Amount"].sum() + vout["To Amount"].sum() + vout["Fee"].sum() < 1:
        temp_df = vout.iloc[[-1], :].copy()
        temp_df["From Amount"] = -(
            vout["From Amount"].sum() + vout["To Amount"].sum() + vout["Fee"].sum()
        )
        temp_df["Tag"] = "Reward"
        temp_df["From"] = temp_df["To"] = None
        temp_df["Fee"] = 0
        temp_df.index -= pd.Timedelta(hours=1)
        vout = pd.concat([vout, temp_df])
        vout.sort_index(inplace=True)

    vout = tx.price_transactions_df(vout, Prices())
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
    return vout
