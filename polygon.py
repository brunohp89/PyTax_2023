import os
import json
from PricesClass import Prices
import datetime as dt
import pandas as pd
import requests
import tax_library as tx

scam_tokens = [
    "0x4e35B8B5DEFf8786AAE88EDD8875ee8aa1d5d7C4",
    "0x4e35b8b5deff8786aae88edd8875ee8aa1d5d7c4",
    "0x0a76fadf1f51654670a8fa11e6cc5cc7af2d18e4",
    "0x0000000000000000000000000000000000000001",
    "0x0000000000000000000000000000000000000000",
    "0xaf6b1a3067bb5245114225556e5b7a52cf002752",
    "0xba12222222228d8ba445958a75a0704d566bf2c8",
    "0x214d52880b1e4e17d020908cd8eaa988ffdd4020",
    "0x794a61358d6845594f94dc1db02a252b5b4814ad",
    "0xe592427a0aece92de3edee1f18e0157c05861564",
]


def get_nfts(address):
    # ERC721 and ERC1155 tokens, prezzi non in MATIC non funzionano ancora e saranno mostrati come zero
    # NFTS ricevuti non attraverso una transazione vengono esclusi
    with open(os.getcwd() + "\\.json") as creds:
        api_key = json.load(creds)["PolygonScanToken"]

    if api_key == "":
        raise PermissionError("No API KEY for Polygon Scan found in .json")

    address = address.lower()
    url = f"https://api.polygonscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)

    normal_transactions_in = pd.DataFrame(response.json().get("result"))

    url = f"https://api.polygonscan.com/api?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    erc721_transactions = pd.DataFrame(response.json().get("result"))

    url = f"https://api.polygonscan.com/api?module=account&action=token1155tx&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    ERC1155_transactions_in = pd.DataFrame(response.json().get("result"))
    erc721_transactions = pd.concat([erc721_transactions, ERC1155_transactions_in])

    if erc721_transactions.shape[0] == 0 and ERC1155_transactions_in.shape[0] == 0:
        print(f"No NFT found for this MATICereum address - {address}")
        return None

    erc721_transactions = pd.merge(
        normal_transactions_in,
        erc721_transactions,
        on="hash",
        suffixes=("-Trx", "-NFT"),
    )

    erc721_transactions["value"] = [
        -int(erc721_transactions.loc[i, "value"]) / 10**18
        if erc721_transactions.loc[i, "from-Trx"] == address.lower()
        else int(erc721_transactions.loc[i, "value"]) / 10**18
        for i in range(erc721_transactions.shape[0])
    ]

    nft_transactions = pd.DataFrame()
    nft_transactions.index = erc721_transactions["timeStamp-Trx"].map(
        lambda x: dt.datetime.fromtimestamp(int(x))
    )
    nft_transactions["NFT"] = list(
        erc721_transactions["tokenName"] + " - " + erc721_transactions["tokenID"]
    )
    nft_transactions["Price buy (MATIC)"] = erc721_transactions["value"].tolist()
    nft_transactions["Price sold (MATIC)"] = erc721_transactions["value"].tolist()
    nft_transactions["Price buy (EUR)"] = 0
    nft_transactions["Price sold (EUR)"] = 0
    nft_transactions["Fee (MATIC)"] = [
        -(
            int(erc721_transactions.loc[i, "gasUsed-Trx"])
            * int(erc721_transactions.loc[i, "gasPrice-Trx"])
        )
        / 10**18
        for i in range(erc721_transactions.shape[0])
    ]
    nft_transactions["Fee (EUR)"] = 0

    nft_prices = Prices()
    nft_prices.get_prices(["MATIC"])
    nft_prices.convert_prices(["MATIC"], "EUR")

    temp_df = nft_transactions.copy()
    temp_df.index = [k.date() for k in nft_transactions.index]
    fiat_prices = pd.merge(
        nft_prices.prices["Prices"]["EUR"]["MATIC"][["Open", "Close", "High", "Low"]],
        temp_df,
        how="right",
        left_index=True,
        right_index=True,
    )
    fiat_prices = list(fiat_prices.iloc[:, 0:4].mean(axis=1))

    nft_transactions["MATIC Price"] = fiat_prices
    nft_transactions.loc[
        nft_transactions["Price sold (MATIC)"] < 0, "Price sold (MATIC)"
    ] = 0
    nft_transactions.loc[
        nft_transactions["Price buy (MATIC)"] > 0, "Price buy (MATIC)"
    ] = 0

    nft_transactions["Price buy (EUR)"] = (
        nft_transactions["Price buy (MATIC)"] * nft_transactions["MATIC Price"]
    )
    nft_transactions["Price sold (EUR)"] = (
        nft_transactions["Price sold (MATIC)"] * nft_transactions["MATIC Price"]
    )

    nft_transactions["Fee (EUR)"] = (
        nft_transactions["Fee (MATIC)"] * nft_transactions["MATIC Price"]
    )

    nft_transactions["Total sold (MATIC)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price sold (MATIC)"], nft_transactions["Fee (MATIC)"]
        )
    ]
    nft_transactions["Total sold (EUR)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price sold (EUR)"], nft_transactions["Fee (EUR)"]
        )
    ]
    nft_transactions["Total buy (MATIC)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price buy (MATIC)"], nft_transactions["Fee (MATIC)"]
        )
    ]
    nft_transactions["Total buy (EUR)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price buy (EUR)"], nft_transactions["Fee (EUR)"]
        )
    ]

    nft_transactions = nft_transactions[
        [
            "NFT",
            "Total buy (MATIC)",
            "Total buy (EUR)",
            "Total sold (MATIC)",
            "Total sold (EUR)",
            "Price buy (MATIC)",
            "Price buy (EUR)",
            "Price sold (MATIC)",
            "Price sold (EUR)",
            "Fee (MATIC)",
            "Fee (EUR)",
            "MATIC Price",
        ]
    ]
    nft_transactions[
        [
            "Total buy (MATIC)",
            "Total buy (EUR)",
            "Total sold (MATIC)",
            "Total sold (EUR)",
            "Price buy (MATIC)",
            "Price buy (EUR)",
            "Price sold (MATIC)",
            "Price sold (EUR)",
            "Fee (MATIC)",
            "Fee (EUR)",
            "MATIC Price",
        ]
    ] = nft_transactions[
        [
            "Total buy (MATIC)",
            "Total buy (EUR)",
            "Total sold (MATIC)",
            "Total sold (EUR)",
            "Price buy (MATIC)",
            "Price buy (EUR)",
            "Price sold (MATIC)",
            "Price sold (EUR)",
            "Fee (MATIC)",
            "Fee (EUR)",
            "MATIC Price",
        ]
    ].applymap(
        lambda x: abs(x)
    )

    return nft_transactions


def get_transactions_df(address):
    with open(os.getcwd() + "\\.json") as creds:
        api_key = json.load(creds)["PolygonScanToken"]

    if api_key == "":
        raise PermissionError("No API KEY for Polygon Scan found in .json")

    # NORMAL TRANSACTIONS
    url = f"https://api.polygonscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    normal_transactions = pd.DataFrame(response.json().get("result"))
    normal_transactions = normal_transactions[normal_transactions["isError"] == "0"]
    normal_transactions.reset_index(inplace=True, drop=True)
    normal_transactions["Coin"] = "MATIC"

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
    url = f"https://api.polygonscan.com/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
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
    url = f"https://api.polygonscan.com/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={api_key}"
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

    all_trx["value-N"] = all_trx["value-N"].combine_first(
        all_trx["value-I"]
    )  # .combine_first(all_trx['value'])

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

    all_trx = all_trx[~all_trx["tokenSymbol"].str.contains("UNI-V2", na=False)].copy()

    all_trx.loc[
        all_trx["functionName"].str.contains("exit", na=False), "Tag"
    ] = "Reward"  # Removing from Farms

    liquidity_df = all_trx[all_trx["functionName"].str.contains("Liquidity", na=False)]
    all_trx = all_trx[~all_trx["hash"].isin(list(liquidity_df["hash"]))]
    for token in set(liquidity_df["tokenSymbol"]):
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token]
        if len(temp_df[temp_df["functionName"].str.contains("add", na=False)]) > len(
            temp_df[temp_df["functionName"].str.contains("remove", na=False)]
        ):
            temp_df = temp_df.iloc[0:-1, :]
        coin1 = temp_df["value-N"].sum()
        coin2 = temp_df["value"].sum()
        index_remove = temp_df.index[-1]
        temp_df = temp_df.iloc[0:2, :]
        temp_df["Coin"] = ["MATIC", token]
        temp_df["tokenSymbol"] = None
        temp_df["value"] = None
        temp_df["value-N"] = [coin1, coin2]
        temp_df["Tag"] = "Reward"
        temp_df["Notes"] = "Liquidity Pool"
        temp_df.index = [index_remove] * 2
        all_trx = pd.concat([all_trx, temp_df])

    all_trx.loc[
        all_trx["functionName"].str.contains("swap|multicall", na=False), "Tag"
    ] = "Trade"

    multicall_df = all_trx.loc[all_trx["functionName"] == "multicall"]
    for hash in set(multicall_df["hash"]):
        if multicall_df[multicall_df["hash"] == hash].shape[0] > 1:
            all_trx = all_trx[all_trx["hash"] != hash]
            temp_df = multicall_df[multicall_df["hash"] == hash].copy()
            temp_df["value"] = temp_df["value"].sum()
            all_trx = pd.concat([all_trx, temp_df.iloc[[0], :]])

    all_trx.loc[all_trx["Tag"] == "", "Tag"] = "Movement"
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
    all_trx["Fee Coin"] = "MATIC"
    all_trx["Fee Fiat"] = None
    all_trx["Source"] = f"MATIC-{address[0:5]}"

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

    vout.loc[vout["To"] == address.lower(), "Fee"] = 0
    vout["Fee"] += 0.59 / vout.shape[0]

    return vout
