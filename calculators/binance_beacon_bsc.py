import os
import numpy as np
from PricesClass import Prices
import tax_library as tx
import datetime as dt
import pandas as pd
import requests
import json

scam_tokens = [
    "0xf3822314b333cbd7a36753b77589afbe095df1ba",
    "0x0df62d2cd80591798721ddc93001afe868c367ff",
    "0xb0557906c617f0048a700758606f64b33d0c41a6",
    "0xb8a9704d48c3e3817cc17bc6d350b00d7caaecf6",
    "0x5558447b06867ffebd87dd63426d61c868c45904",
    "0xd22202d23fe7de9e3dbe11a2a88f42f4cb9507cf",
    "0xab57aef3601cad382aa499a6ae2018a69aad9cf0",
    "0x5190b01965b6e3d786706fd4a999978626c19880",
    "0x569b2cf0b745ef7fad04e8ae226251814b3395f9",
    "0x8ee3e98dcced9f5d3df5287272f0b2d301d97c57",
    "0xbc6675de91e3da8eac51293ecb87c359019621cf",
    "0x64f2c2aa04755507a2ecd22ceb8c475b7a750a3a",
    "0x9028418bbf045fcfe85a3d44ab8054712d98872b",
    "0x4a5fad6631fd3df66f23519608185cb96e9a687d",
    "0x0b7dc561777842d55163e0f48886295aad1359b9",
    "0x5f7a1a4dafd0718caee1184caa4862543f75edb1",
    "0x0000000000000000000000000000000000000000",
    "0x55d398326f99059ff775485246999027b3197955",
]


def get_transactions_df(address, beacon_address=None, burn_meccanism_coins=None):
    # FOR A CERTAIN GENERATION OF SHITCOINS ON BNB CHAIN (i.e. SAFEMOON BASED SHITCOINS) IT IS IMPOSSIBLE
    # TO CALCULATE PRECISELY THE AMOUNTS BECAUSE OF SOME CONSTANT INTERNAL BURN MECHANISMS AND CONSTANT REWARDS
    # OF BURNT TOKENS TO HOLDERS. CALCULATE FIRT THE BALANCE WITH burn_meccanism_coins = None, CALCULATE
    # THE DIFFERENCE BETWEEN THE REAL BALANCE AND THE CALCULATION THEN, PASS TO THE FUNCTION, A DICTIONARY
    # WITH THE COINS AS KEYS AND THE AMOUNT BURNED, A FAKE TRANSACTION WILL BE PUT IN PLACED; IT WILL BE TAGGED AS SHITCOIN
    with open(os.getcwd() + "\\.json") as creds:
        api_key = json.load(creds)["BSCScanToken"]

    if api_key == "":
        raise PermissionError("No API KEY for BSC Scan found in .json")

    address = address.lower()

    if beacon_address is not None:
        beacon = tx.get_bnb(beacon_address)

    # NORMAL TRANSACTIONS
    url = f"https://api.bscscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    normal_transactions = pd.DataFrame(response.json().get("result"))
    normal_transactions = normal_transactions[normal_transactions["isError"] == "0"]
    normal_transactions.reset_index(inplace=True, drop=True)
    normal_transactions["Coin"] = "BNB"

    normal_transactions["from"] = normal_transactions["from"].map(lambda x: x.lower())
    normal_transactions["to"] = normal_transactions["to"].map(lambda x: x.lower())
    normal_transactions["value"] = [
        -int(normal_transactions.loc[i, "value"]) / 10**18
        if normal_transactions.loc[i, "from"] == address.lower()
        else int(normal_transactions.loc[i, "value"]) / 10**18
        for i in range(normal_transactions.shape[0])
    ]
    normal_transactions["gas"] = [
        -(
            int(normal_transactions.loc[i, "gasUsed"])
            * int(normal_transactions.loc[i, "gasPrice"])
        )
        / 10**18
        for i in range(normal_transactions.shape[0])
    ]

    normal_transactions["functionName"] = normal_transactions["functionName"].map(
        lambda x: x.split("(")[0]
    )

    normal_transactions.drop(
        [
            "blockNumber",
            "blockHash",
            "nonce",
            "transactionIndex",
            "gasPrice",
            "input",
            "gasUsed",
            "methodId",
            "contractAddress",
            "isError",
            "cumulativeGasUsed",
            "confirmations",
            "txreceipt_status",
        ],
        axis=1,
        inplace=True,
    )

    # INTERNAL TRANSACTIONS
    url = f"https://api.bscscan.com/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response_internal = requests.get(url)
    internal_transactions = pd.DataFrame(response_internal.json().get("result"))
    internal_transactions = internal_transactions[
        internal_transactions["isError"] == "0"
    ]
    internal_transactions.reset_index(inplace=True, drop=True)

    internal_transactions["from"] = internal_transactions["from"].map(
        lambda x: x.lower()
    )
    internal_transactions["to"] = internal_transactions["to"].map(lambda x: x.lower())
    internal_transactions["value"] = [
        -int(internal_transactions.loc[i, "value"]) / 10**18
        if internal_transactions.loc[i, "from"] == address.lower()
        else int(internal_transactions.loc[i, "value"]) / 10**18
        for i in range(internal_transactions.shape[0])
    ]

    internal_transactions.drop(
        [
            "blockNumber",
            "contractAddress",
            "input",
            "type",
            "gas",
            "gasUsed",
        ],
        axis=1,
        inplace=True,
    )

    # CRC20 TRANSACTIONS
    url = f"https://api.bscscan.com/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    erc20_transactions = pd.DataFrame(response.json().get("result"))

    erc20_transactions["from"] = erc20_transactions["from"].map(lambda x: x.lower())
    erc20_transactions["to"] = erc20_transactions["to"].map(lambda x: x.lower())

    erc20_transactions.reset_index(inplace=True, drop=True)

    erc20_transactions["value"] = [
        int(s) / 10 ** int(x)
        for s, x in zip(erc20_transactions["value"], erc20_transactions["tokenDecimal"])
    ]
    erc20_transactions["gas"] = [
        -(
            int(erc20_transactions.loc[i, "gasUsed"])
            * int(erc20_transactions.loc[i, "gasPrice"])
        )
        / 10**18
        for i in range(erc20_transactions.shape[0])
    ]

    erc20_transactions = erc20_transactions[
        ~erc20_transactions["from"].isin(scam_tokens)
    ]

    erc20_transactions.drop(
        [
            "blockNumber",
            "tokenDecimal",
            "nonce",
            "blockHash",
            "transactionIndex",
            "gasPrice",
            "contractAddress",
            "cumulativeGasUsed",
            "gasUsed",
            "confirmations",
            "input",
            "tokenName",
        ],
        axis=1,
        inplace=True,
    )

    erc20_transactions.loc[erc20_transactions["from"] == address.lower(), "value"] *= -1

    # -------------------------------------------------------------------------------------------------------------------

    all_trx = pd.merge(
        normal_transactions,
        internal_transactions,
        how="outer",
        on="hash",
        suffixes=("-N", "-I"),
    )
    all_trx = pd.merge(
        all_trx, erc20_transactions, how="outer", on="hash", suffixes=("", "-C")
    )

    all_trx.loc[all_trx["value-N"] == 0, "value-N"] = None

    all_trx["value-N"] = all_trx["value-N"].combine_first(all_trx["value-I"])

    all_trx.index = [
        dt.datetime.fromtimestamp(int(x))
        for x in list(
            all_trx["timeStamp-N"]
            .combine_first(all_trx["timeStamp-I"])
            .combine_first(all_trx["timeStamp"])
        )
    ]

    all_trx = all_trx[
        [
            "hash",
            "from-N",
            "to-N",
            "value-N",
            "Coin",
            "value",
            "tokenSymbol",
            "gas",
            "functionName",
        ]
    ].copy()
    all_trx["Tag"] = ""
    all_trx["Notes"] = ""

    all_trx.sort_index(inplace=True)

    # SWAPS
    swaps_df = all_trx[all_trx["functionName"].str.contains("swap", na=False)].copy()
    for hash in swaps_df["hash"]:
        if swaps_df[swaps_df["hash"] == hash].shape[0] > 1:
            temp_df = swaps_df[swaps_df["hash"] == hash].copy()
            all_trx = all_trx[all_trx["hash"] != hash]
            temp_df["Coin"] = list(temp_df["tokenSymbol"])[::-1]
            temp_df["value-N"] = list(temp_df["value"])[::-1]
            all_trx = pd.concat([all_trx, temp_df.iloc[[0], :]])

    all_trx.loc[
        all_trx["tokenSymbol"].str.contains("\*", regex=True, na=False), "value"
    ] = None
    all_trx.loc[
        all_trx["tokenSymbol"].str.contains("\*", regex=True, na=False), "tokenSymbol"
    ] = None
    all_trx = all_trx[~all_trx["tokenSymbol"].str.contains("-LP", regex=True, na=False)]
    all_trx = all_trx[~all_trx["tokenSymbol"].str.contains("-LP", regex=True, na=False)]

    all_trx.loc[
        np.logical_and(
            all_trx["tokenSymbol"] == "STG", all_trx["functionName"] == "withdraw"
        ),
        "Tag",
    ] = "Reward"  # Removing from Stargate

    all_trx.loc[
        all_trx["functionName"] == "instantRedeemLocal", "value"
    ] *= 0  # Removing movements for stargate LP

    liquidity_df = all_trx[all_trx["functionName"].str.contains("Liquidity", na=False)]
    all_trx = all_trx[~all_trx["hash"].isin(list(liquidity_df["hash"]))]
    for token in set(liquidity_df["tokenSymbol"]):
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        if (
            temp_df[temp_df["functionName"].str.contains("remove", na=False)].shape[0]
            == 0
        ):
            continue
        if len(temp_df[temp_df["functionName"].str.contains("add", na=False)]) > len(
            temp_df[temp_df["functionName"].str.contains("remove", na=False)]
        ):
            temp_df = temp_df.iloc[0:-1, :]
        coin1 = temp_df["value-N"].sum()
        coin2 = temp_df["value"].sum()
        index_remove = temp_df.index[-1]
        temp_df = temp_df.iloc[0:2, :]
        temp_df["Coin"] = ["BNB", token]
        temp_df["tokenSymbol"] = None
        temp_df["value"] = None
        temp_df["value-N"] = [coin1, coin2]
        temp_df["Tag"] = "Reward"
        temp_df["Notes"] = "Liquidity Pool"
        temp_df.index = [index_remove] * 2
        new_gas = [0] * (temp_df.shape[0] - 1)
        new_gas.append(temp_df["gas"].tolist()[0])
        temp_df["gas"] = new_gas
        all_trx = pd.concat([all_trx, temp_df])

    # PANCAKE SWAP CAKE SYRUP POOL
    pancake_df = all_trx.loc[
        all_trx[
            np.logical_or(
                all_trx["to-N"].isin(
                    [
                        "0xa80240eb5d7e05d3f250cf000eec0891d00b51cc",
                        "0x45c54210128a065de780c4b0df3d16664f7f859e",
                    ]
                ),
                all_trx["from-N"].isin(
                    [
                        "0xa80240eb5d7e05d3f250cf000eec0891d00b51cc",
                        "0x45c54210128a065de780c4b0df3d16664f7f859e",
                    ]
                ),
            )
        ].index,
        :,
    ]
    if pancake_df.shape[0] > 0:
        pancake_df.drop_duplicates(inplace=True)
        all_trx = all_trx[~all_trx["hash"].isin(pancake_df["hash"])]
        profit = None

        if "withdrawAll" in list(pancake_df["functionName"]):
            profit = pancake_df["value"].sum()
        elif "harvest" in list(pancake_df["functionName"]):
            profit = pancake_df.loc[
                pancake_df["functionName"] == "harvest", "value"
            ].sum()

        if profit is not None:
            pancake_df["gas"] = pancake_df["gas"].sum()
            profit_trx = pancake_df.iloc[[0], :].copy()
            profit_trx["Tag"] = "Reward"
            profit_trx["Notes"] = "Pancake Syrup Pool"
            profit_trx["tokenSymbol"] = "Cake"
            profit_trx["value"] = profit
            all_trx = pd.concat([all_trx, profit_trx])
        else:
            all_trx = pd.concat([all_trx, pancake_df])

    all_trx.loc[
        all_trx["to-N"] == "0xd4888870c8686c748232719051b677791dbda26d", "value"
    ] *= 0  # stargate lockup

    all_trx.loc[
        all_trx["functionName"].str.contains("swap|multicall", na=False), "Tag"
    ] = "Trade"

    all_trx.loc[all_trx["Tag"] == "", "Tag"] = "Movement"
    all_trx.loc[
        all_trx["functionName"].str.contains("airdrop", na=False), "Tag"
    ] = "Reward"
    all_trx.loc[
        all_trx["functionName"].str.contains("airdrop", na=False), "Notes"
    ] = "Airdrop"

    all_trx.drop(["functionName", "hash"], axis=1, inplace=True)

    all_trx.columns = [
        "From",
        "To",
        "From Amount",
        "From Coin",
        "To Amount",
        "To Coin",
        "Fee",
        "Tag",
        "Notes",
    ]

    all_trx["Fiat Price"] = None
    all_trx["Fiat"] = "EUR"
    all_trx["Fee Coin"] = "BNB"
    all_trx["Fee Fiat"] = None
    all_trx["Source"] = f"BSC-{address[0:5]}"

    vout = all_trx.copy()

    vout.sort_index(inplace=True)

    sub1 = vout.loc[vout["From Amount"] > 0, ["From Amount", "From Coin"]]
    sub2 = vout.loc[vout["From Amount"] > 0, ["To Amount", "To Coin"]]
    vout.loc[vout["From Amount"] > 0, ["To Amount", "To Coin"]] = sub1.values
    vout.loc[vout["From Amount"] > 0, ["From Amount", "From Coin"]] = sub2.values

    vout["From Coin"] = vout["From Coin"].apply(
        lambda x: None if pd.isna(x) else x.upper()
    )
    vout["To Coin"] = vout["To Coin"].apply(lambda x: None if pd.isna(x) else x.upper())

    vout.loc[vout["To"] == address, "Fee"] *= 0

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

    vout.sort_index(inplace=True)

    vout.loc[
        vout["To"] == "0x0000000000000000000000000000000000001004", "To Amount"
    ] *= 0

    vout.loc[
        np.logical_and(~pd.isna(vout["From Coin"]), pd.isna(vout["From Amount"])),
        "From Coin",
    ] = None
    vout.loc[
        np.logical_and(~pd.isna(vout["To Coin"]), pd.isna(vout["To Amount"])), "To Coin"
    ] = None

    if burn_meccanism_coins is not None:
        for shit_coin in burn_meccanism_coins.keys():
            ind = [
                max(
                    max(vout[vout["To Coin"] == shit_coin].index, default=0),
                    max(
                        vout[vout["From Coin"] == shit_coin].index,
                        default=pd.Timestamp("1900-01-01"),
                    ),
                )
            ]
            temp_df = vout.iloc[[0], :].copy()
            temp_df["From Coin"] = shit_coin
            temp_df["From Amount"] = -burn_meccanism_coins[shit_coin]
            temp_df["Fee"] = temp_df["Fee Coin"] = temp_df["Fee Fiat"] = temp_df[
                "To Amount"
            ] = temp_df["To Coin"] = temp_df["Notes"] = None
            temp_df["Tag"] = "Shit Coin"
            temp_df.index = ind
            vout = pd.concat([vout, temp_df])
        vout.sort_index(inplace=True)

    vout = tx.price_transactions_df(vout, Prices())
    return vout
