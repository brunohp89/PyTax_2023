import os
import json
import datetime as dt
import numpy as np
import pandas as pd
import requests
from PricesClass import Prices
import tax_library as tx

scam_tokens = [
    "0x1883a07c429e84aca23b041c357e1d21a2b645f3",
    "0x59728544b08ab483533076417fbbb2fd0b17ce3a",
]


def get_nfts(address):
    # NFTS ricevuti non attraverso una transazione vengono esclusi
    with open(os.getcwd() + "\\.json") as creds:
        apikey = json.load(creds)["ETHScanToken"]

    if apikey == "":
        raise PermissionError("No API KEY for ETH Scan found in .json")

    address = address.lower()
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)

    normal_transactions_in = pd.DataFrame(response.json().get("result"))

    url = f"https://api.etherscan.io/api?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)
    erc721_transactions = pd.DataFrame(response.json().get("result"))

    url = f"https://api.etherscan.io/api?module=account&action=token1155tx&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)
    ERC1155_transactions_in = pd.DataFrame(response.json().get("result"))
    erc721_transactions = pd.concat([erc721_transactions, ERC1155_transactions_in])

    if erc721_transactions.shape[0] == 0 and ERC1155_transactions_in.shape[0] == 0:
        print(f"No NFT found for this ethereum address - {address}")
    # return None

    erc721_transactions = pd.merge(
        normal_transactions_in,
        erc721_transactions,
        on="hash",
        suffixes=("-Trx", "-NFT"),
    )
    erc721_transactions["Token"] = None

    # Get ERC20 tokens
    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey=GFID2HN2QCS6UR4K1CX13F946P2V1S7Q7X"
    response = requests.get(url)
    erc20_transactions = pd.DataFrame(response.json().get("result"))
    if erc20_transactions.shape[0] > 0:
        erc721_transactions = pd.merge(
            erc721_transactions,
            erc20_transactions,
            on="hash",
            suffixes=("", "-ERC20"),
            how="left",
        )

        erc721_transactions.loc[erc721_transactions["value"] != "0", "Token"] = "ETH"
        erc721_transactions["Token"] = erc721_transactions["Token"].combine_first(
            erc721_transactions["tokenSymbol-ERC20"]
        )
        erc721_transactions.loc[erc721_transactions["value"] == "0", "value"] = None
        erc721_transactions["value"] = erc721_transactions["value"].combine_first(
            erc721_transactions["value-ERC20"]
        ).apply(lambda x: float(x)) / erc721_transactions["tokenDecimal-ERC20"].apply(
            lambda x: 10**18 if pd.isna(x) else 10 ** int(x)
        )
        erc721_transactions["value"] = [
            -erc721_transactions.loc[i, "value"]
            if erc721_transactions.loc[i, "to-NFT"] == address.lower()
            else erc721_transactions.loc[i, "value"]
            for i in range(erc721_transactions.shape[0])
        ]
    else:
        erc721_transactions["value"] = [
            -int(erc721_transactions.loc[i, "value"]) / 10**18
            if erc721_transactions.loc[i, "to-NFT"] == address.lower()
            else int(erc721_transactions.loc[i, "value"]) / 10**18
            for i in range(erc721_transactions.shape[0])
        ]

    nft_transactions = pd.DataFrame()
    nft_transactions["NFT"] = list(
        erc721_transactions["tokenName"] + " - " + erc721_transactions["tokenID"]
    )
    nft_transactions["timeStamp"] = erc721_transactions["timeStamp-Trx"].map(
        lambda x: dt.datetime.fromtimestamp(int(x))
    )
    nft_transactions["Price"] = erc721_transactions["value"].tolist()
    nft_transactions["Asset"] = erc721_transactions["Token"].tolist()
    nft_transactions["Fee (ETH)"] = [
        -(
            int(erc721_transactions.loc[i, "gasUsed-Trx"])
            * int(erc721_transactions.loc[i, "gasPrice-Trx"])
        )
        / 10**18
        for i in range(erc721_transactions.shape[0])
    ]
    nft_buys = nft_transactions[nft_transactions["Price"] < 0]
    nft_sells = nft_transactions[nft_transactions["Price"] > 0]
    if nft_sells.shape[0] > 0:
        nft_transactions = pd.merge(nft_buys, nft_sells, on="NFT", how="outer")
        nft_transactions.columns = [
            "NFT",
            "Time Buy",
            "Price buy (Asset)",
            "Asset Buy",
            "Gas Buy (ETH)",
            "Time Sell",
            "Price Sell (Asset)",
            "Asset Sell",
            "Gas Sell (ETH)",
        ]
    else:
        nft_transactions = nft_buys.copy()
        nft_transactions.columns = [
            "NFT",
            "Time Buy",
            "Price buy (Asset)",
            "Asset Buy",
            "Gas Buy (ETH)",
        ]
        nft_transactions["Price Sell (Asset)"] = None
        nft_transactions["Asset Sell"] = None
        nft_transactions["Gas Sell (ETH)"] = None
        nft_transactions["Time Sell"] = None

    nft_transactions["Price buy (EUR)"] = 0
    nft_transactions["Price sold (EUR)"] = 0

    nft_transactions["Fee Buy (EUR)"] = 0
    nft_transactions["Fee Sell (EUR)"] = 0

    assets = nft_transactions["Asset Sell"].tolist()
    assets.extend(nft_transactions["Asset Buy"])
    assets.append("ETH")
    assets = [k for k in set(assets) if not pd.isna(k)]

    nft_prices = Prices()
    nft_prices.get_prices(assets)
    nft_prices.convert_prices(assets, "EUR")

    nft_transactions["Date Buy"] = [k.date() for k in nft_transactions["Time Buy"]]
    nft_transactions["Date Sell"] = [k.date() for k in nft_transactions["Time Sell"]]
    nft_transactions["Asset Price Buy"] = None
    nft_transactions["Asset Price Sell"] = None

    for i, asset in enumerate(assets):
        dates = [
            k.date()
            for k in nft_transactions.loc[
                nft_transactions["Asset Buy"] == asset, "Time Buy"
            ]
        ]
        dates.extend(
            [
                k.date()
                for k in nft_transactions.loc[
                    nft_transactions["Asset Sell"] == asset, "Time Sell"
                ]
            ]
        )
        temp_df = pd.DataFrame()
        temp_df.index = temp_df["Date"] = temp_df["Date Buy"] = temp_df[
            "Date Sell"
        ] = dates
        fiat_prices = pd.merge(
            nft_prices.prices["Prices"]["EUR"][asset.upper()][
                ["Open", "Close", "High", "Low"]
            ],
            temp_df,
            how="right",
            left_index=True,
            right_index=True,
        )
        fiat_prices = list(fiat_prices.iloc[:, 0:4].mean(axis=1))
        temp_df["Prices"] = fiat_prices
        temp_df.drop_duplicates(inplace=True)
        if nft_transactions[nft_transactions["Asset Buy"] == asset].shape[0] > 0:
            temp_df2 = pd.merge(
                nft_transactions.loc[
                    nft_transactions["Asset Buy"] == asset, "Date Buy"
                ],
                temp_df[["Date Buy", "Prices"]],
                on="Date Buy",
                how="left",
                suffixes=("", "-BUY"),
            )
            nft_transactions.loc[
                nft_transactions["Asset Buy"] == asset, "Asset Price Buy"
            ] = temp_df2["Prices"].tolist()
        if nft_transactions[nft_transactions["Asset Sell"] == asset].shape[0] > 0:
            temp_df2 = pd.merge(
                nft_transactions.loc[
                    nft_transactions["Asset Sell"] == asset, "Date Sell"
                ],
                temp_df[["Date Sell", "Prices"]],
                on="Date Sell",
                how="outer",
                suffixes=("", "-SELL"),
            )
            nft_transactions.loc[
                nft_transactions["Asset Sell"] == asset, "Asset Price Sell"
            ] = temp_df2["Prices"].tolist()

    dates = nft_transactions["Date Buy"].tolist()
    dates.extend(nft_transactions["Date Sell"].tolist())
    dates = [k for k in dates if not pd.isna(k)]
    temp_df = pd.DataFrame()
    temp_df.index = temp_df["Date Buy"] = temp_df["Date Sell"] = dates
    fiat_prices = pd.merge(
        nft_prices.prices["Prices"]["EUR"]["ETH"][["Open", "Close", "High", "Low"]],
        temp_df,
        how="right",
        left_index=True,
        right_index=True,
    )
    fiat_prices = list(fiat_prices.iloc[:, 0:4].mean(axis=1))
    temp_df["Prices"] = fiat_prices
    temp_df.drop_duplicates(inplace=True)
    nft_transactions = pd.merge(
        nft_transactions, temp_df[["Date Buy", "Prices"]], on="Date Buy", how="left"
    )
    nft_transactions = pd.merge(
        nft_transactions, temp_df[["Date Sell", "Prices"]], on="Date Sell", how="left"
    )

    nft_transactions.drop(["Date Sell", "Date Buy"], axis=1, inplace=True)

    nft_transactions.rename(
        columns={"Prices_x": "Gas Price Buy (EUR)", "Prices_y": "Gas Price Sell (EUR)"},
        inplace=True,
    )

    nft_transactions["Price buy (EUR)"] = (
        nft_transactions["Asset Price Buy"] * -nft_transactions["Price buy (Asset)"]
    )
    nft_transactions["Price sold (EUR)"] = (
        nft_transactions["Asset Price Sell"] * nft_transactions["Price Sell (Asset)"]
    )

    nft_transactions["Fee Buy (EUR)"] = (
        nft_transactions["Gas Price Buy (EUR)"] * -nft_transactions["Gas Buy (ETH)"]
    )
    nft_transactions["Fee Sell (EUR)"] = (
        nft_transactions["Gas Price Sell (EUR)"] * -nft_transactions["Gas Sell (ETH)"]
    )

    nft_transactions["Total Buy (EUR)"] = (
        nft_transactions["Price buy (EUR)"] + nft_transactions["Fee Buy (EUR)"]
    )
    nft_transactions["Total Sell (EUR)"] = (
        nft_transactions["Price sold (EUR)"] - nft_transactions["Fee Sell (EUR)"]
    )

    nft_transactions["Total PNL (EUR)"] = (
        nft_transactions["Total Sell (EUR)"] - nft_transactions["Total Buy (EUR)"]
    )

    nft_transactions.columns = [
        "NFT",
        "Time Bought",
        "Price Bought (Crypto)",
        "Crypto Buy",
        "Gas Fee Buy (ETH)",
        "Time Sell",
        "Price Sold (Crypto)",
        "Crypto Sell",
        "Gas Fee Sell (ETH)",
        "Price Bought (EUR)",
        "Price Sold (EUR)",
        "Gas Fee Buy (EUR)",
        "Gas Fee Sell (EUR)",
        "Crypto Price Buy (EUR)",
        "Crypto Price Sell (EUR)",
        "Gas Price Buy (EUR)",
        "Gas Price Sell (EUR)",
        "Total Cost Buy (EUR)",
        "Total Cost Sell (EUR)",
        "Final PNL (EUR)",
    ]

    nft_transactions.fillna(0, inplace=True)

    nft_transactions[
        [
            "Price Bought (Crypto)",
            "Gas Fee Buy (ETH)",
            "Price Sold (Crypto)",
            "Gas Fee Sell (ETH)",
        ]
    ] = nft_transactions[
        [
            "Price Bought (Crypto)",
            "Gas Fee Buy (ETH)",
            "Price Sold (Crypto)",
            "Gas Fee Sell (ETH)",
        ]
    ].apply(
        lambda x: abs(x)
    )

    nft_transactions[
        [
            "Price Bought (EUR)",
            "Price Sold (EUR)",
            "Gas Fee Buy (EUR)",
            "Gas Fee Sell (EUR)",
            "Crypto Price Buy (EUR)",
            "Crypto Price Sell (EUR)",
            "Gas Price Buy (EUR)",
            "Gas Price Sell (EUR)",
            "Total Cost Buy (EUR)",
            "Total Cost Sell (EUR)",
        ]
    ] = nft_transactions[
        [
            "Price Bought (EUR)",
            "Price Sold (EUR)",
            "Gas Fee Buy (EUR)",
            "Gas Fee Sell (EUR)",
            "Crypto Price Buy (EUR)",
            "Crypto Price Sell (EUR)",
            "Gas Price Buy (EUR)",
            "Gas Price Sell (EUR)",
            "Total Cost Buy (EUR)",
            "Total Cost Sell (EUR)",
        ]
    ].applymap(
        lambda x: round(abs(x), 2)
    )
    nft_transactions["Final PNL (EUR)"] = nft_transactions["Final PNL (EUR)"].apply(
        lambda x: round(x, 2)
    )
    return nft_transactions


def get_transactions_df(address):
    with open(os.getcwd() + "\\.json") as creds:
        api_key = json.load(creds)["ETHScanToken"]

    if api_key == "":
        raise PermissionError("No API KEY for ETH Scan found in .json")

    address = address.lower()
    # NORMAL TRANSACTIONS
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    normal_transactions = pd.DataFrame(response.json().get("result"))
    normal_transactions = normal_transactions[normal_transactions["isError"] == "0"]
    normal_transactions.reset_index(inplace=True, drop=True)
    normal_transactions["Coin"] = "ETH"

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
    url = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={api_key}"
    response_internal = requests.get(url)
    internal_transactions = pd.DataFrame(response_internal.json().get("result"))
    if internal_transactions.shape[0] > 0:
        internal_transactions = internal_transactions[
            internal_transactions["isError"] == "0"
        ]
        internal_transactions.reset_index(inplace=True, drop=True)

        internal_transactions["from"] = internal_transactions["from"].map(
            lambda x: x.lower()
        )
        internal_transactions["to"] = internal_transactions["to"].map(
            lambda x: x.lower()
        )
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
    else:
        internal_transactions = pd.DataFrame(
            columns=["timeStamp", "hash", "from", "to", "value", "tokenSymbol", "gas"]
        )

    # CRC20 TRANSACTIONS
    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    erc20_transactions = pd.DataFrame(response.json().get("result"))
    if erc20_transactions.shape[0] > 0:
        erc20_transactions["from"] = erc20_transactions["from"].map(lambda x: x.lower())
        erc20_transactions["to"] = erc20_transactions["to"].map(lambda x: x.lower())

        erc20_transactions.reset_index(inplace=True, drop=True)

        erc20_transactions["value"] = [
            int(s) / 10 ** int(x)
            for s, x in zip(
                erc20_transactions["value"], erc20_transactions["tokenDecimal"]
            )
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

        erc20_transactions.loc[
            erc20_transactions["from"] == address.lower(), "value"
        ] *= -1
    else:
        erc20_transactions = pd.DataFrame(
            columns=["timeStamp", "hash", "from", "to", "value", "tokenSymbol", "gas"]
        )
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

    all_trx.loc[
        np.logical_and(
            all_trx["to-N"] == "0x00000000006c3852cbef3e08e8df289169ede581",
            all_trx["tokenSymbol"] == "WETH",
        ),
        ["value", "tokenSymbol"],
    ] = None

    if "tokenSymbol-C" in all_trx.columns:
        all_trx["Coin"] = (
            all_trx["tokenSymbol-C"]
            .combine_first(all_trx["tokenSymbol"])
            .combine_first(all_trx["Coin"])
        )

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
        temp_df["Coin"] = ["ETH", token]
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
    all_trx["Fee Coin"] = "ETH"
    all_trx["Fee Fiat"] = None
    all_trx["Source"] = f"ETH-{address[0:5]}"

    all_trx.loc[
        np.logical_and(pd.isna(all_trx["To Coin"]), ~pd.isna(all_trx["To Amount"])),
        "To Coin",
    ] = all_trx.loc[
        np.logical_and(pd.isna(all_trx["To Coin"]), ~pd.isna(all_trx["To Amount"])),
        "From Coin",
    ]
    all_trx.loc[pd.isna(all_trx["From Amount"]), "From Coin"] = None

    outdf = tx.price_transactions_df(all_trx, Prices())

    outdf = outdf[
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

    # Payout address of COIN (XYO foundation)
    outdf.loc[
        outdf["From"] == "0x4aef1fd68c9d0b17d85e0f4e90604f6c92883f18", "Tag"
    ] = "Reward"

    return outdf
