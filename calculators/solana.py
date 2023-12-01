from PricesClass import Prices
import datetime as dt
import pandas as pd
import requests
import tax_library as tx
import os
import numpy as np
import json
import math

scam = ["6GnNtx93PwLwxQJtdW3g1kUpbqFvxRWqNmyqdxHG5yTV","FLiPggWYQyKVTULFWMQjAk26JfK5XRCajfyTmD5weaZ7", "Habp5bncMSsBC3vkChyebepym5dcTNRYeg2LVG464E96"]


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
        lambda x: dt.datetime.fromtimestamp(int(x["blockTime"]))
    )
    response_pd = response_pd.sort_index()
    response_pd["filt"] = response_pd["data"].apply(
        lambda x: str(x)
    )

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
                / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
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
                -transaction["meta"]["fee"] / 10**9
            ) / 2
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "Tag"] = "Reward"

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )

            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "To Amount"] = -[
                (y - x) / 10**9
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

        elif any(
            [
                True if x in scam else False
                for x in [
                    x["pubkey"]
                    for x in transaction["transaction"]["message"]["accountKeys"]
                ]
            ]
        ):
            response_pd = response_pd[response_pd["data"] != transaction]

        elif "TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp" in ",".join(
            transaction["meta"]["logMessages"]
        ) and "Instruction: Buy" in ",".join(transaction["meta"]["logMessages"]):
            response_pd.loc[response_pd["data"] == transaction, "From"] = address
            response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[response_pd["data"] == transaction, "From Amount"] = (
                -sum(
                    [
                        x["parsed"]["info"]["lamports"]
                        for x in transaction["meta"]["innerInstructions"][0][
                            "instructions"
                        ]
                        if x["programId"] == "11111111111111111111111111111111"
                    ]
                )
                / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "To Amount"] = 1
            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = [
                x["accounts"][0]
                for x in transaction["meta"]["innerInstructions"][0]["instructions"]
                if x["programId"] == "TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp"
            ][0]
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Buy NFT tensor"

            nfts.append(
                [
                    x["accounts"][0]
                    for x in transaction["meta"]["innerInstructions"][0]["instructions"]
                    if x["programId"] == "TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp"
                ][0]
            )

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]

        elif "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" in ",".join(
            transaction["meta"]["logMessages"]
        ):
            if (
                len(
                    [
                        x["mint"]
                        for x in transaction["meta"]["postTokenBalances"]
                        if x["owner"] == address
                        and x["uiTokenAmount"]["uiAmount"] is not None
                    ]
                )
                > 1
            ):
                print(
                    f"Attention, tokens swaps not being processed. Txid: {transaction['transaction']['signatures'][0]}"
                )
                continue

            token = [
                x["mint"]
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address and x["uiTokenAmount"]["uiAmount"] is not None
            ][0]
            token = requests.get(f"https://api.solana.fm/v0/tokens/{token}").json()[
                "result"
            ]["data"]["symbol"]

            if (
                len(
                    [
                        float(x["uiTokenAmount"]["uiAmount"])
                        for x in transaction["meta"]["preTokenBalances"]
                        if x["owner"] == address
                        and x["uiTokenAmount"]["uiAmount"] is not None
                    ]
                )
                > 0
            ):
                amount1 = [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["postTokenBalances"]
                    if x["owner"] == address
                    and x["uiTokenAmount"]["uiAmount"] is not None
                ][0] - [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["preTokenBalances"]
                    if x["owner"] == address
                    and x["uiTokenAmount"]["uiAmount"] is not None
                ][
                    0
                ]
            else:
                amount1 = [
                    float(x["uiTokenAmount"]["uiAmount"])
                    for x in transaction["meta"]["postTokenBalances"]
                    if x["owner"] == address
                    and x["uiTokenAmount"]["uiAmount"] is not None
                ][0]
            amount2 = [
                (x - y) / 10**9
                for x, y in zip(
                    transaction["meta"]["postBalances"],
                    transaction["meta"]["preBalances"],
                )
            ][0]

            if amount1 < 0:
                response_pd.loc[
                    response_pd["data"] == transaction, "From Amount"
                ] = amount1
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = token
                response_pd.loc[response_pd["data"] == transaction, "To Amount"] = abs(
                    amount2
                )
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = "SOL"
            else:
                response_pd.loc[
                    response_pd["data"] == transaction, "To Amount"
                ] = amount1
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = token
                response_pd.loc[
                    response_pd["data"] == transaction, "From Amount"
                ] = -abs(amount2)
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"

            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "Notes"] = "Orca Swap"

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]

        elif "TerraformHabitat" in str(transaction):
            sol_price = -abs(
                [
                    (x - y) / 10**9
                    for x, y in zip(
                        transaction["meta"]["postBalances"],
                        transaction["meta"]["preBalances"],
                    )
                ][17]
                + [
                    (x - y) / 10**9
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
            temp_df["Fee"] = (-transaction["meta"]["fee"] / 10**9) / 6
            temp_df["Fee Coin"] = "SOL"
            temp_df["Notes"] = "Buy NFT - Create Habitat Genopets"

            response_pd = response_pd[response_pd["data"] != transaction]

            response_pd = pd.concat([temp_df, response_pd])

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]

        elif "ray_log" in str(transaction):
            response_pd.loc[
                response_pd["data"] == transaction, ["Fee Coin", "Fee", "Notes"]
            ] = ["SOL", -transaction["meta"]["fee"] / 10**9, "Raydium Incomplete"]
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "RemoveSubHabitat" in str(transaction) or "AddSubHabitat" in str(
            transaction
        ):
            response_pd.loc[
                response_pd["data"] == transaction, ["Fee Coin", "Fee", "Notes"]
            ] = [
                "SOL",
                -transaction["meta"]["fee"] / 10**9,
                "Genopet Subhabitat Management",
            ]
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]

        elif "Instruction: BuyV2" in str(transaction):  # Buy NFT MagicEden
            response_pd.loc[response_pd["data"] == transaction, "From"] = address
            response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[response_pd["data"] == transaction, "From Amount"] = (
                -sum(
                    [
                        x["parsed"]["info"]["lamports"]
                        for x in transaction["meta"]["innerInstructions"][0][
                            "instructions"
                        ]
                        if x["programId"] == "11111111111111111111111111111111"
                    ]
                )
                / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "To Amount"] = 1
            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = [
                x["mint"]
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address
            ][0]
            nfts.append(
                [
                    x["mint"]
                    for x in transaction["meta"]["postTokenBalances"]
                    if x["owner"] == address
                ][0]
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Buy NFT MagicEden"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]

        elif "srmv4uTCPF81hWDaPyEN2mLZ8XbvzuEM6LsAxR8NpjU" in str(transaction):
            response_pd.loc[response_pd["data"] == transaction, "From"] = transaction[
                "transaction"
            ]["message"]["accountKeys"][-1]["pubkey"]
            response_pd.loc[response_pd["data"] == transaction, "To"] = address
            response_pd.loc[response_pd["data"] == transaction, "From Amount"] = -abs(
                [
                    (x - y) / 10**9
                    for x, y in zip(
                        transaction["meta"]["postBalances"],
                        transaction["meta"]["preBalances"],
                    )
                ][4]
                + [
                    x["parsed"]["info"]["lamports"]
                    for x in transaction["transaction"]["message"]["instructions"]
                    if x["programId"] == "11111111111111111111111111111111"
                ][0]
                / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
            response_pd.loc[response_pd["data"] == transaction, "To Amount"] = [
                x["uiTokenAmount"]["uiAmount"]
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address
                and x["mint"] != "So11111111111111111111111111111111111111112"
            ][0]
            response_pd.loc[response_pd["data"] == transaction, "To Coin"] = [
                x["mint"]
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address
                and x["mint"] != "So11111111111111111111111111111111111111112"
            ][0]
            nfts.append(
                [
                    x["mint"]
                    for x in transaction["meta"]["postTokenBalances"]
                    if x["owner"] == address
                    and x["mint"] != "So11111111111111111111111111111111111111112"
                ][0]
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Create Object Genopet"

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
                for x in transaction["meta"]["postTokenBalances"]
                if x["owner"] == address
                and x["mint"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc"
            ][0] - [
                x["uiTokenAmount"]["uiAmount"]
                for x in transaction["meta"]["preTokenBalances"]
                if x["owner"] == address
                and x["mint"] == "kiGenopAScF8VF31Zbtx2Hg8qA5ArGqvnVtXb83sotc"
            ][
                0
            ]
            response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "KI"
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
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
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Repair Habitat Genopets"
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
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Refine Crystals Genopets"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "STEPNq2UGeGSzCyGVr2nMQAzf8xuejwqebd84wcksCK" in str(transaction) and 'Program log: Instruction: Transfer' in str(transaction):
            tokens = calculate_tokens_balances(transaction,address)
            token = requests.get(f"https://api.solana.fm/v0/tokens/{tokens.tokens[0]}").json()["result"]["data"]["symbol"].replace('-SOL','')
            if tokens.result[0] > 0:
                response_pd.loc[response_pd["data"] == transaction, "From"] = transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "To"] = address
                response_pd.loc[response_pd["data"] == transaction, "To Amount"] = tokens.result[0]
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = token
            else:
                response_pd.loc[response_pd["data"] == transaction, "To"] = transaction["transaction"]["message"]["accountKeys"][-2]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "From"] = address
                response_pd.loc[response_pd["data"] == transaction, "Fee"] = (-transaction["meta"]["fee"] / 10 ** 9)
                response_pd.loc[response_pd["data"] == transaction, "From Amount"] = tokens.result[0]
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = token
            response_pd.loc[response_pd["data"] == transaction, "Notes"] = 'Stepn Transfers'
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "STEPNq2UGeGSzCyGVr2nMQAzf8xuejwqebd84wcksCK" in str(transaction) and 'Program 11111111111111111111111111111111 invoke' in str(transaction):
            if transaction["transaction"]["message"]["accountKeys"][0]["pubkey"] == address:
                response_pd.loc[response_pd["data"] == transaction, "From"] = transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "To"] = transaction["transaction"]["message"]["accountKeys"][-2]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "From Amount"] = -transaction["transaction"]["message"]['instructions'][0]['parsed']['info']['lamports']/10**9
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = 'SOL'
                response_pd.loc[response_pd["data"] == transaction, "Fee"] = (-transaction["meta"]["fee"] / 10 ** 9)
            else:
                response_pd.loc[response_pd["data"] == transaction, "To"] = address
                response_pd.loc[response_pd["data"] == transaction, "From"] = transaction["transaction"]["message"]["accountKeys"][0]["pubkey"]
                response_pd.loc[response_pd["data"] == transaction, "To Amount"] = transaction["transaction"]["message"]['instructions'][0]['parsed']['info']['lamports']/10**9
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = 'SOL'
            response_pd.loc[response_pd["data"] == transaction, "Notes"] = 'Stepn Transfers'
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "SysvarRent111111111111111111111111111111111" in str(transaction):
            response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                -transaction["meta"]["fee"] / 10**9
            )
            response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Inner Transactions"

            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        elif "'type': 'transfer'}}" in str(transaction):
            if (
                transaction["transaction"]["message"]["instructions"][-1]["parsed"][
                    "info"
                ]["source"]
                == address
            ):
                response_pd.loc[response_pd["data"] == transaction, "From"] = address
                response_pd.loc[response_pd["data"] == transaction, "From Amount"] = (
                    -transaction["transaction"]["message"]["instructions"][-1][
                        "parsed"
                    ]["info"]["lamports"]
                    / 10**9
                )
                response_pd.loc[response_pd["data"] == transaction, "From Coin"] = "SOL"
                response_pd.loc[response_pd["data"] == transaction, "Fee"] = (
                    -transaction["meta"]["fee"] / 10**9
                )
                response_pd.loc[response_pd["data"] == transaction, "Fee Coin"] = "SOL"
            else:
                response_pd.loc[
                    response_pd["data"] == transaction, "From"
                ] = transaction["transaction"]["message"]["instructions"][-1]["parsed"][
                    "info"
                ][
                    "source"
                ]

            if (
                transaction["transaction"]["message"]["instructions"][-1]["parsed"][
                    "info"
                ]["destination"]
                == address
            ):
                response_pd.loc[response_pd["data"] == transaction, "To"] = address
                response_pd.loc[response_pd["data"] == transaction, "To Amount"] = (
                    transaction["transaction"]["message"]["instructions"][-1]["parsed"][
                        "info"
                    ]["lamports"]
                    / 10**9
                )
                response_pd.loc[response_pd["data"] == transaction, "To Coin"] = "SOL"
            else:
                response_pd.loc[response_pd["data"] == transaction, "To"] = transaction[
                    "transaction"
                ]["message"]["instructions"][-1]["parsed"]["info"]["destination"]

            response_pd.loc[
                response_pd["data"] == transaction, "Notes"
            ] = "Sol transfer"
            final_df = pd.concat(
                [final_df, response_pd[response_pd["data"] == transaction]]
            )
            response_pd = response_pd[response_pd["data"] != transaction]
        else:
            print(f"Transaction not being processed, check!")

    final_df.loc[final_df["To Coin"].isin(nfts), "To Amount"] = None
    final_df.loc[final_df["To Coin"].isin(nfts), "To Coin"] = None
    final_df.loc[final_df["From Coin"].isin(nfts), "From Amount"] = None
    final_df.loc[final_df["From Coin"].isin(nfts), "From Coin"] = None

    final_df["Source"] = f"'Solana-{address[0:10]}"
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

    if f"{address}.csv" in os.listdir():
        manual = pd.read_csv(f"{address}.csv", parse_dates=True, index_col="Timestamp")
        final_df = pd.concat([manual, final_df])
    final_df = final_df.sort_index()

    sol_prices = Prices()
    final_df = tx.price_transactions_df(final_df, sol_prices)

    return final_df