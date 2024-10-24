from PricesClass import Prices
import datetime as dt
import pandas as pd
import requests
import tax_library as tx
import os
import numpy as np
import json
import math
from utils import date_from_timestamp

scam = [
    "6GnNtx93PwLwxQJtdW3g1kUpbqFvxRWqNmyqdxHG5yTV",
    "FLiPggWYQyKVTULFWMQjAk26JfK5XRCajfyTmD5weaZ7",
    "Habp5bncMSsBC3vkChyebepym5dcTNRYeg2LVG464E96",
]


def calculate_tokens_balances(transaction, address):
    tokens = pd.DataFrame()
    tokens["post_bal"] = [
        x["uiTokenAmount"]["uiAmount"]
        if x["uiTokenAmount"]["uiAmount"] is not None
        else 0
        for x in transaction["meta"]["postTokenBalances"]
        if x["owner"] == address
    ]

    tokens["tokens"] = [
        x["mint"] if x["uiTokenAmount"]["uiAmount"] is not None else 0
        for x in transaction["meta"]["postTokenBalances"]
        if x["owner"] == address
    ]

    tokens2 = pd.DataFrame()

    tokens2["pre_bal"] = [
        x["uiTokenAmount"]["uiAmount"]
        if x["uiTokenAmount"]["uiAmount"] is not None
        else 0
        for x in transaction["meta"]["preTokenBalances"]
        if x["owner"] == address
    ]

    tokens2["tokens"] = [
        x["mint"] if x["uiTokenAmount"]["uiAmount"] is not None else 0
        for x in transaction["meta"]["preTokenBalances"]
        if x["owner"] == address
    ]

    if all([True if isinstance(x, int) else False for x in tokens2["tokens"]]):
        tokens2["tokens"] = tokens["tokens"]

    if all([True if isinstance(x, int) else False for x in tokens["tokens"]]):
        tokens["tokens"] = tokens2["tokens"]

    tokens = pd.merge(tokens, tokens2, on="tokens", how="outer").fillna(0)
    tokens["result"] = tokens["post_bal"] - tokens["pre_bal"]

    return tokens


def get_transactions_df(address):
    nfts = []
    transactions = requests.get(
        f"https://api.solana.fm/v0/accounts/{address}/transactions?utcFrom=1611839871&utcTo={int(dt.datetime.now().timestamp())}"
    )
    data = [x["signature"] for x in json.loads(transactions.text)["result"]["data"]]

    transactions_content = []
    for i in range(math.ceil(len(data) / 50)):
        if (i + 1) * 50 > len(data):
            end = len(data)
        else:
            end = (i + 1) * 50
        start = i * 50
        transactions_content.extend(
            requests.post(
                "https://api.solana.fm/v0/transactions",
                json={"transactionHashes": data[start:end]},
            ).json()["result"]
        )

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
    response_pd["Fiat"] = "EUR"
    response_pd["Tag"] = None
    response_pd["Notes"] = ""
    response_pd["Fiat Price"] = None
    response_pd.index = response_pd["data"].apply(
        lambda x: date_from_timestamp(int(x["blockTime"]))
    )
    response_pd = response_pd.sort_index()
    response_pd["filt"] = response_pd["data"].apply(lambda x: str(x))

    final_df = pd.DataFrame()

    for transaction in response_pd["data"]:
        if "HarvestKi" in ",".join(transaction["meta"]["logMessages"]):
            response_pd.loc[response_pd["data"] == transaction, "From"] = address
            response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[response_pd["data"] == transaction, "From Amount"] = (
                    -transaction["meta"]["innerInstructions"][0]["instructions"][0][
                        "parsed"
                    ]["info"]["lamports"]
                    / 10 ** 9
            )
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                    -transaction["meta"]["fee"] / 10 ** 9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Genopet Harvest Ki"

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "WithdrawKi" in ",".join(transaction["meta"]["logMessages"]):
            tokens = calculate_tokens_balances(transaction, address)

            response_pd.loc[response_pd["data"] == transaction, "To"] = address
            response_pd.loc[response_pd["data"] == transaction, "From"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[
                response_pd["data"] == transaction, "To Amount"
            ] = tokens.loc[
                tokens["tokens"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                "result",
            ].values[
                0
            ]
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Genopet Mint Ki"
            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = "KI"
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                                                                                 -transaction["meta"]["fee"] / 10 ** 9
                                                                         ) / 2
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "Tag"] = "Reward"

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )

            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "To Amount"] = -[
                (y - x) / 10 ** 9
                for x, y in zip(
                    transaction["meta"]["postBalances"],
                    transaction["meta"]["preBalances"],
                )
            ][0]
            response_pd[["From Coin", "From Amount", "Tag"]] = None

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "TerraformHabitat" in str(transaction):
            sol_price = -abs(
                [
                    (x - y) / 10 ** 9
                    for x, y in zip(
                    transaction["meta"]["postBalances"],
                    transaction["meta"]["preBalances"],
                )
                ][17]
                + [
                    (x - y) / 10 ** 9
                    for x, y in zip(
                        transaction["meta"]["postBalances"],
                        transaction["meta"]["preBalances"],
                    )
                ][11]
            )
            ki_price = -abs(
                [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["postTokenBalances"]
                    if x["owner"] == address
                       and x["mint"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc"
                ][0]
                - [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["preTokenBalances"]
                    if x["owner"] == address
                       and x["mint"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc"
                ][0]
            )
            gene_price = -abs(
                [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["postTokenBalances"]
                    if x["owner"] == address
                       and x["mint"] == "GENEtH5amGSi8kHAtQoezp1XEXwZJ8vcuePYnXdKrMYz"
                ][0]
                - [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["preTokenBalances"]
                    if x["owner"] == address
                       and x["mint"] == "GENEtH5amGSi8kHAtQoezp1XEXwZJ8vcuePYnXdKrMYz"
                ][0]
            )
            other = [
                x["uiTokenAmount"]["uiAmount"]
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address
                   and x["mint"]
                   not in [
                       "GENEtH5amGSi8kHAtQoezp1XEXwZJ8vcuePYnXdKrMYz",
                       "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                   ]
            ]
            other2 = [
                x["uiTokenAmount"]["uiAmount"]
                for x in transaction["meta"]["preTokenBalances"]
                if x["owner"] == address
                   and x["mint"]
                   not in [
                       "GENEtH5amGSi8kHAtQoezp1XEXwZJ8vcuePYnXdKrMYz",
                       "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                   ]
            ]
            other_prices = [
                x - y if x is not None else 0 - y for x, y in zip(other, other2)
            ]
            other_assets = [
                x["mint"]
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address
                   and x["mint"]
                   not in [
                       "GENEtH5amGSi8kHAtQoezp1XEXwZJ8vcuePYnXdKrMYz",
                       "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                   ]
            ]
            nfts.extend(other_assets)

            temp_df = pd.DataFrame(
                np.repeat(
                    response_pd[response_pd["data"] == transaction].values, 6, axis=0
                ),
                columns=response_pd.columns,
            )
            for i, x in enumerate(zip(other_prices, other_assets)):
                if x[0] < 0:
                    temp_df.loc[i, "From Amount"] = x[0]
                    temp_df.loc[i, "From Coin"] = x[1]
                else:
                    temp_df.loc[i, "To Amount"] = x[0]
                    temp_df.loc[i, "To Coin"] = x[1]
            temp_df.loc[temp_df["To Amount"] == 0, "To Amount"] = 1

            temp_df.loc[3, "From Amount"] = sol_price
            temp_df.loc[4, "From Amount"] = ki_price
            temp_df.loc[5, "From Amount"] = gene_price
            temp_df.loc[3, "From Coin"] = "SOL"
            temp_df.loc[4, "From Coin"] = "KI"
            temp_df.loc[5, "From Coin"] = "GENE"
            temp_df.index = [
                response_pd[response_pd["data"] == transaction].index[0]
                + dt.timedelta(milliseconds=x * 100)
                for x in range(6)
            ]
            temp_df["Fee"] = (-transaction["meta"]["fee"] / 10 ** 9) / 6
            temp_df["Fee Coin"] = "SOL"
            temp_df["Notes"] = "Buy NFT - Create Habitat Genopets"

            response_pd = response_pd[response_pd["data"] != transaction]

            response_pd = pd.concat([temp_df, response_pd])

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif (
                "RemoveSubHabitat" in str(transaction)
                or "AddSubHabitat" in str(transaction)
                or "WithdrawHabitat" in str(transaction)
        ):
            response_pd.loc[
                response_pd["data"] == transaction, ["Fee Coin", "Fee", "Notes"]
            ] = [
                "SOL",
                -transaction["meta"]["fee"] / 10 ** 9,
                "Genopet Subhabitat Management",
            ]
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "ConvertKiToEnergy" in str(transaction):
            response_pd.loc[response_pd["data"] == transaction, "From"] = address
            response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[response_pd["data"] == transaction, "From Amount"] = [
                                                                                     x["uiTokenAmount"]["uiAmount"]
                                                                                     for x in transaction["meta"][
                    "postTokenBalances"]
                                                                                     if x["owner"] == address
                                                                                        and x[
                                                                                            "mint"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc"
                                                                                 ][0] - [
                                                                                     x["uiTokenAmount"]["uiAmount"]
                                                                                     for x in transaction["meta"][
                    "preTokenBalances"]
                                                                                     if x["owner"] == address
                                                                                        and x[
                                                                                            "mint"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc"
                                                                                 ][
                                                                                     0
                                                                                 ]
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "KI"
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                    -transaction["meta"]["fee"] / 10 ** 9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Convert Ki to Energy Genopets"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "RepairHabitat" in str(transaction):
            # calculate_tokens_balances(transaction,address)
            response_pd.loc[response_pd["data"] == transaction, "From"] = address
            response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[response_pd["data"] == transaction, "From Amount"] = [
                x - y
                for x, y in zip(
                    [
                        x["uiTokenAmount"]["uiAmount"]
                        if x["uiTokenAmount"]["uiAmount"] is not None
                        else 0
                        for x in transaction["meta"]["postTokenBalances"]
                        if x["owner"] == address
                    ],
                    [
                        x["uiTokenAmount"]["uiAmount"]
                        if x["uiTokenAmount"]["uiAmount"] is not None
                        else 0
                        for x in transaction["meta"]["preTokenBalances"]
                        if x["owner"] == address
                    ],
                )
            ][0]
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = [
                x["mint"] if x["uiTokenAmount"]["uiAmount"] is not None else 0
                for x in transaction["meta"]["preTokenBalances"]
                if x["owner"] == address
            ][0]
            nfts.append(
                [
                    x["mint"] if x["uiTokenAmount"]["uiAmount"] is not None else 0
                    for x in transaction["meta"]["preTokenBalances"]
                    if x["owner"] == address
                ][0]
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                    -transaction["meta"]["fee"] / 10 ** 9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Repair Habitat Genopets"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "ReviveHabitat" in str(transaction):
            tokens = calculate_tokens_balances(transaction, address)
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "KI"
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                    -transaction["meta"]["fee"] / 10 ** 9
            )
            response_pd.loc[
                response_pd["data"] == transaction, "From Amount"
            ] = tokens.result[0]
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Revive Habitat Genopets"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "RefineCrystals" in str(transaction):
            response_pd.loc[response_pd["data"] == transaction, "From"] = address
            response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]

            tokens = calculate_tokens_balances(transaction, address)

            response_pd.loc[
                response_pd["data"] == transaction, "From Amount"
            ] = tokens.loc[
                tokens["tokens"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                "result",
            ].values[
                0
            ]
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "KI"
            response_pd.loc[
                response_pd["data"] == transaction, "To Amount"
            ] = tokens.loc[
                tokens["tokens"] != "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                "result",
            ].values[
                0
            ]
            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = tokens.loc[
                tokens["tokens"] != "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                "tokens",
            ][0]

            nfts.append(
                tokens.loc[
                    tokens["tokens"] != "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc",
                    "tokens",
                ][0]
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                    -transaction["meta"]["fee"] / 10 ** 9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Refine Crystals Genopets"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "STEPN" in str(transaction) and "Program log: Instruction: Transfer" in str(transaction):
            tokens = calculate_tokens_balances(transaction, address)
            token = (
                requests.get(f"https://api.solana.fm/v0/tokens/{tokens.tokens[0]}")
                .json()["result"]["data"]["symbol"]
                .replace("-SOL", "")
            )
            if tokens.result[0] > 0:
                response_pd.loc[
                    response_pd["data"] == transaction, "From"
                ] = transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "To"] = address
                response_pd.loc[
                    response_pd["data"] == transaction, "To Amount"
                ] = tokens.result[0]
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = token
            else:
                response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                    "transaction"
                ]["message"]["accountKeys"][-2]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "From"] = address
                response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                        -transaction["meta"]["fee"] / 10 ** 9
                )
                response_pd.loc[
                    response_pd["data"] == transaction, "From Amount"
                ] = tokens.result[0]
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = token
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Stepn Transfers"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "STEPN" in str(
                transaction
        ) and "Program 11111111111111111111111111111111 invoke" in str(transaction):
            if (
                    transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                    == address
            ):
                response_pd.loc[
                    response_pd["data"] == transaction, "From"
                ] = transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                    "transaction"
                ]["message"]["accountKeys"][-2]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "From Amount"] = (
                        -transaction["transaction"]["message"]["instructions"][0]["parsed"][
                            "info"
                        ]["lamports"]
                        / 10 ** 9
                )
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
                response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                        -transaction["meta"]["fee"] / 10 ** 9
                )
            else:
                response_pd.loc[response_pd["data"] == transaction, "To"] = address
                response_pd.loc[
                    response_pd["data"] == transaction, "From"
                ] = transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "To Amount"] = (
                        transaction["transaction"]["message"]["instructions"][0]["parsed"][
                            "info"
                        ]["lamports"]
                        / 10 ** 9
                )
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Stepn Transfers"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        else:
            continue

    final_df.loc[final_df["To Coin"].isin(nfts), "To Amount"] = None
    final_df.loc[final_df["To Coin"].isin(nfts), "To Coin"] = None
    final_df.loc[final_df["From Coin"].isin(nfts), "From Amount"] = None
    final_df.loc[final_df["From Coin"].isin(nfts), "From Coin"] = None

    final_df["Source"] = f"Solana-{address[0:10]}"
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

    if f"{address}.csv" in os.listdir(os.path.join('input')):
        manual = pd.read_csv(f"input/{address}.csv", parse_dates=True, index_col="Timestamp")
        final_df = pd.concat([manual, final_df])
    final_df = final_df.sort_index()
    final_df["Fee Coin"] = "SOL"

    final_df.loc[np.logical_and(final_df['Notes'] == "Stepn Transfers", final_df['To Amount'] > 0), 'Tag'] = 'Reward'
    final_df['Tag'] = final_df['Tag'].fillna('Movement')

    sol_prices = Prices()

    temp_nft = final_df[np.logical_or(final_df['From Coin'].str.contains('->', na=False),
                                      final_df['To Coin'].str.contains('->', na=False))].copy()
    final_df.loc[np.logical_or(final_df['From Coin'].str.contains('->', na=False),
                               final_df['To Coin'].str.contains('->', na=False)), 'New Tag'] = '!'

    final_df.loc[final_df['From Coin'].str.contains('->', na=False), ['From Coin', 'From Amount']] = None
    final_df.loc[final_df['To Coin'].str.contains('->', na=False), ['To Coin', 'To Amount']] = None

    final_df = tx.price_transactions_df(final_df, sol_prices)

    final_df.loc[final_df['New Tag'] == '!', ['From Coin', 'From Amount', 'To Coin', 'To Amount']] = temp_nft[
        ['From Coin', 'From Amount', 'To Coin', 'To Amount']].values
    final_df['Fee'] = final_df['Fee'].infer_objects(copy=False).fillna(0)
    final_df['Fee Fiat'] = final_df['Fee Fiat'].infer_objects(copy=False).fillna(0)

    final_df = final_df.drop('New Tag', axis=1)

    final_df['Fee'] = final_df['Fee'].astype(float)
    final_df['From Amount'] = final_df['From Amount'].astype(float)
    final_df['To Amount'] = final_df['To Amount'].astype(float)

    final_df['Source'] = f'SOL-{address[0:10]}'

    final_df = final_df.sort_index()

    return final_df
