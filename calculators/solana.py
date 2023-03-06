import json
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

    with open(os.getcwd() + "\\.json") as creds:
        apikey = json.load(creds)["SolScanAPIToken"]

    if apikey == "":
        raise PermissionError("No API KEY for SolScan found in .json")

    for transaction in set(tokens_transactions["Txhash"]):
        resp_loop = requests.get(
            f"https://public-api.solscan.io/transaction/{transaction}",
            headers={'accept': 'application/json', 'token': f'{apikey}'}
        )
        i = 0
        while i <= 3 and resp_loop.status_code != 200:
            resp_loop = requests.get(
                f"https://public-api.solscan.io/transaction/{transaction}",
                headers={'accept': 'application/json', 'token': f'{apikey}'}
            )
        contents.append(resp_loop.json())
        hashes.append(transaction)
    contents_pd["Txhash"] = hashes
    contents_pd["Txcontent"] = contents

    return contents_pd


def get_transactions_df(address):
    os.makedirs(f"{os.getcwd()}\\solana", exist_ok=True)

    with open(os.getcwd() + "\\.json") as creds:
        apikey = json.load(creds)["SolScanAPIToken"]

    if apikey == "":
        raise PermissionError("No API KEY for SolScan found in .json")

    response = requests.get(
        f"https://public-api.solscan.io/account/exportTransactions?account={address}&type=all&fromTime=1611839871&toTime={int(dt.datetime.now().timestamp())}",
        headers={'accept': 'application/json', 'token': f'{apikey}'}
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

        # STEPN Transactions ONLY are going to be correctly calculated
        for transaction in set(tokens_transactions["Txhash"]):
            trx_content = tokens_transactions.loc[
                tokens_transactions["Txhash"] == transaction, "Txcontent"
            ].tolist()[0]

            temp_df = tokens_transactions[
                          tokens_transactions["Txhash"] == transaction
                          ].iloc[[0], :]
            tokens_transactions = tokens_transactions[
                tokens_transactions["Txhash"] != transaction
                ]
            if 'Create' in trx_content['logMessage'][1]:
                if 'transfer' in trx_content['parsedInstruction'][-1]['name']:

                    from_account = trx_content['parsedInstruction'][-1]['extra']['sourceOwner']
                    to_account = trx_content['parsedInstruction'][-1]['extra']['destinationOwner']
                    to_amount, from_amount, to_coin, from_coin = (None, None, None, None)
                    if from_account == address:
                        from_coin = trx_content['parsedInstruction'][-1]['extra']['symbol']
                        fee = -trx_content['fee'] / (10 ** 6)
                        from_amount = -int(trx_content['parsedInstruction'][-1]['extra']['amount']) / (
                                10 ** int(trx_content['parsedInstruction'][-1]['extra']['decimals']))
                    else:
                        to_coin = trx_content['parsedInstruction'][-1]['extra']['symbol']
                        to_amount = int(trx_content['parsedInstruction'][-1]['extra']['amount']) / (
                                10 ** int(trx_content['parsedInstruction'][-1]['extra']['decimals']))
                        fee = 0

                    temp_df['From'] = from_account
                    temp_df['To'] = to_account
                    temp_df["From Coin"] = from_coin
                    temp_df["To Coin"] = to_coin
                    temp_df["From Amount"] = from_amount
                    temp_df["To Amount"] = to_amount
                    temp_df["Fee"] = fee
                    if 'STEPN' in from_account:
                        temp_df["Tag"] = "Reward"
                    else:
                        temp_df["Tag"] = "Movement"
                    temp_df["Notes"] = "STEPN exchange"

                else:
                    continue

            elif '111111' in trx_content['logMessage'][1]:

                if len(trx_content['innerInstructions']) > 0:
                    from_amount = int(
                        trx_content['innerInstructions'][0]['parsedInstructions'][0]['params']['amount']) / (10 **
                                                                                                             trx_content[
                                                                                                                 'innerInstructions'][
                                                                                                                 0][
                                                                                                                 'parsedInstructions'][
                                                                                                                 0][
                                                                                                                 'extra'][
                                                                                                                 'decimals'])
                    from_coin = trx_content['innerInstructions'][0]['parsedInstructions'][0]['extra']['symbol']
                    to_amount = int(
                        trx_content['innerInstructions'][-1]['parsedInstructions'][-1]['params']['amount']) / (10 **
                                                                                                               trx_content[
                                                                                                                   'innerInstructions'][
                                                                                                                   -1][
                                                                                                                   'parsedInstructions'][
                                                                                                                   -1][
                                                                                                                   'extra'][
                                                                                                                   'decimals'])
                    to_coin = trx_content['innerInstructions'][-1]['parsedInstructions'][-1]['extra']['symbol']
                    fee = trx_content['fee'] / (10 ** 9)

                    temp_df["From Coin"] = from_coin
                    temp_df["To Coin"] = to_coin
                    temp_df["From Amount"] = -from_amount
                    temp_df["To Amount"] = to_amount
                    temp_df["Fee"] = -fee
                    temp_df["Tag"] = "Trade"
                    temp_df["Notes"] = "STEPN exchange"
                else:
                    from_account = trx_content['parsedInstruction'][-1]['extra']['sourceOwner']
                    to_account = trx_content['parsedInstruction'][-1]['extra']['destinationOwner']
                    to_amount, from_amount, to_coin, from_coin = (None, None, None, None)
                    if from_account == address:
                        from_coin = trx_content['parsedInstruction'][-1]['extra']['symbol']
                        fee = -trx_content['fee'] / (10 ** 6)
                        from_amount = -int(trx_content['parsedInstruction'][-1]['extra']['amount']) / (
                                10 ** int(trx_content['parsedInstruction'][-1]['extra']['decimals']))
                    else:
                        to_coin = trx_content['parsedInstruction'][-1]['extra']['symbol']
                        to_amount = int(trx_content['parsedInstruction'][-1]['extra']['amount']) / (
                                10 ** int(trx_content['parsedInstruction'][-1]['extra']['decimals']))
                        fee = 0

                    temp_df['From'] = from_account
                    temp_df['To'] = to_account
                    temp_df["From Coin"] = from_coin
                    temp_df["To Coin"] = to_coin
                    temp_df["From Amount"] = from_amount
                    temp_df["To Amount"] = to_amount
                    temp_df["Fee"] = fee
                    if 'STEPN' in from_account:
                        temp_df["Tag"] = "Reward"
                    else:
                        temp_df["Tag"] = "Movement"
                    temp_df["Notes"] = "STEPN exchange"


            elif 'MintToChecked' in trx_content['logMessage'][1]:
                continue
            elif 'Transfer' in trx_content['logMessage'][1]:
                from_account = trx_content['parsedInstruction'][-1]['extra']['sourceOwner']
                to_account = trx_content['parsedInstruction'][-1]['extra']['destinationOwner']
                to_amount, from_amount, to_coin, from_coin = (None, None, None, None)
                if from_account == address:
                    from_coin = trx_content['parsedInstruction'][-1]['extra']['symbol']
                    fee = -trx_content['fee'] / (10 ** 6)
                    from_amount = -int(trx_content['parsedInstruction'][-1]['extra']['amount']) / (
                            10 ** int(trx_content['parsedInstruction'][-1]['extra']['decimals']))
                else:
                    to_coin = trx_content['parsedInstruction'][-1]['extra']['symbol']
                    to_amount = int(trx_content['parsedInstruction'][-1]['extra']['amount']) / (
                            10 ** int(trx_content['parsedInstruction'][-1]['extra']['decimals']))
                    fee = 0

                temp_df['From'] = from_account
                temp_df['To'] = to_account
                temp_df["From Coin"] = from_coin
                temp_df["To Coin"] = to_coin
                temp_df["From Amount"] = from_amount
                temp_df["To Amount"] = to_amount
                temp_df["Fee"] = fee
                if 'STEPN' in from_account:
                    temp_df["Tag"] = "Reward"
                else:
                    temp_df["Tag"] = "Movement"
                temp_df["Notes"] = "STEPN exchange"

            elif 'Approve' in trx_content['logMessage'][1]:
                from_amount = int(trx_content['innerInstructions'][0]['parsedInstructions'][0]['params']['amount']) / (
                            10 ** trx_content['innerInstructions'][0]['parsedInstructions'][0]['extra']['decimals'])
                from_coin = trx_content['innerInstructions'][0]['parsedInstructions'][0]['extra']['symbol']
                to_amount = int(trx_content['innerInstructions'][-1]['parsedInstructions'][-1]['params']['amount']) / (
                            10 ** trx_content['innerInstructions'][-1]['parsedInstructions'][-1]['extra']['decimals'])
                to_coin = trx_content['innerInstructions'][-1]['parsedInstructions'][-1]['extra']['symbol']
                fee = trx_content['fee'] / (10 ** 9)

                temp_df["From Coin"] = from_coin
                temp_df["To Coin"] = to_coin
                temp_df["From Amount"] = -from_amount
                temp_df["To Amount"] = to_amount
                temp_df["Fee"] = -fee
                temp_df["Tag"] = "Trade"
                temp_df["Notes"] = "STEPN exchange"
            else:
                print(
                    f"Attention: transaction {transaction} for Solana address {address} is not being correctly calculated"
                )
            tokens_transactions = pd.concat([tokens_transactions, temp_df])

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

        vout = pd.concat([tokens_transactions, normal_transactions])
        vout.sort_index(inplace=True)

        vout.loc[vout["From"].str.contains("STEPN", na=False), "Notes"] = "STEPN"
        vout.loc[vout["To"].str.contains("STEPN", na=False), "Notes"] = "STEPN"

    else:
        vout = normal_transactions.copy()

    vout["Fee"] = vout["Fee"].apply(lambda x: -abs(x))

    # Means that we have some staking rewards
    if round(vout["From Amount"].sum() + vout["To Amount"].sum() + vout["Fee"].sum(), 7) > 0:
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
