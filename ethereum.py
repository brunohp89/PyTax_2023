import os
import json
import datetime as dt
import pandas as pd
import requests
from PricesClass import Prices
import tax_library as tx

scam_tokens = ["0x1883a07c429e84aca23b041c357e1d21a2b645f3"]

def get_nfts(address):
    # ERC721 and ERC1155 tokens, prezzi non in ETH non funzionano ancora e saranno mostrati come zero
    # NFTS ricevuti non attraverso una transazione vengono esclusi
    with open(os.getcwd() + "\\.json") as creds:
        apikey = json.load(creds)["ETHScanToken"]

    if apikey == "":
        raise PermissionError("No API KEY for ETH Scan found in .json")

    address = address.lower()
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)

    normal_transactions_in = pd.DataFrame(response.json().get("result"))

    url = f"https://api.etherscan.io/api?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey=GFID2HN2QCS6UR4K1CX13F946P2V1S7Q7X"
    response = requests.get(url)
    erc721_transactions = pd.DataFrame(response.json().get("result"))

    url = f"https://api.etherscan.io/api?module=account&action=token1155tx&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)
    ERC1155_transactions_in = pd.DataFrame(response.json().get("result"))
    erc721_transactions = pd.concat([erc721_transactions, ERC1155_transactions_in])

    if erc721_transactions.shape[0] == 0 and ERC1155_transactions_in.shape[0] == 0:
        print(f"No NFT found for this ethereum address - {address}")
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
    nft_transactions["Price buy (ETH)"] = erc721_transactions["value"].tolist()
    nft_transactions["Price sold (ETH)"] = erc721_transactions["value"].tolist()
    nft_transactions["Price buy (EUR)"] = 0
    nft_transactions["Price sold (EUR)"] = 0
    nft_transactions["Fee (ETH)"] = [
        -(
            int(erc721_transactions.loc[i, "gasUsed-Trx"])
            * int(erc721_transactions.loc[i, "gasPrice-Trx"])
        )
        / 10**18
        for i in range(erc721_transactions.shape[0])
    ]
    nft_transactions["Fee (EUR)"] = 0

    nft_prices = Prices()
    nft_prices.get_prices(["ETH"])
    nft_prices.convert_prices(["ETH"], "EUR")

    temp_df = nft_transactions.copy()
    temp_df.index = [k.date() for k in nft_transactions.index]
    fiat_prices = pd.merge(
        nft_prices.prices["Prices"]["EUR"]["ETH"][["Open", "Close", "High", "Low"]],
        temp_df,
        how="right",
        left_index=True,
        right_index=True,
    )
    fiat_prices = list(fiat_prices.iloc[:, 0:4].mean(axis=1))

    nft_transactions["ETH Price"] = fiat_prices
    nft_transactions.loc[
        nft_transactions["Price sold (ETH)"] < 0, "Price sold (ETH)"
    ] = 0
    nft_transactions.loc[nft_transactions["Price buy (ETH)"] > 0, "Price buy (ETH)"] = 0

    nft_transactions["Price buy (EUR)"] = (
        nft_transactions["Price buy (ETH)"] * nft_transactions["ETH Price"]
    )
    nft_transactions["Price sold (EUR)"] = (
        nft_transactions["Price sold (ETH)"] * nft_transactions["ETH Price"]
    )

    nft_transactions["Fee (EUR)"] = (
        nft_transactions["Fee (ETH)"] * nft_transactions["ETH Price"]
    )

    nft_transactions["Total sold (ETH)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price sold (ETH)"], nft_transactions["Fee (ETH)"]
        )
    ]
    nft_transactions["Total sold (EUR)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price sold (EUR)"], nft_transactions["Fee (EUR)"]
        )
    ]
    nft_transactions["Total buy (ETH)"] = [
        x + y if x != 0 else 0
        for x, y in zip(
            nft_transactions["Price buy (ETH)"], nft_transactions["Fee (ETH)"]
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
            "Total buy (ETH)",
            "Total buy (EUR)",
            "Total sold (ETH)",
            "Total sold (EUR)",
            "Price buy (ETH)",
            "Price buy (EUR)",
            "Price sold (ETH)",
            "Price sold (EUR)",
            "Fee (ETH)",
            "Fee (EUR)",
            "ETH Price",
        ]
    ]
    nft_transactions[
        [
            "Total buy (ETH)",
            "Total buy (EUR)",
            "Total sold (ETH)",
            "Total sold (EUR)",
            "Price buy (ETH)",
            "Price buy (EUR)",
            "Price sold (ETH)",
            "Price sold (EUR)",
            "Fee (ETH)",
            "Fee (EUR)",
            "ETH Price",
        ]
    ] = nft_transactions[
        [
            "Total buy (ETH)",
            "Total buy (EUR)",
            "Total sold (ETH)",
            "Total sold (EUR)",
            "Price buy (ETH)",
            "Price buy (EUR)",
            "Price sold (ETH)",
            "Price sold (EUR)",
            "Fee (ETH)",
            "Fee (EUR)",
            "ETH Price",
        ]
    ].applymap(
        lambda x: abs(x)
    )

    return nft_transactions


def get_transactions_df(address):
    with open(os.getcwd() + "\\.json") as creds:
        apikey = json.load(creds)["ETHScanToken"]

    if apikey == "":
        raise PermissionError("No API KEY for ETH Scan found in .json")

    address = address.lower()
    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)

    normal_transactions_in = pd.DataFrame(response.json().get("result"))
    normal_transactions = normal_transactions_in[
        normal_transactions_in["isError"] != 1
    ].copy()
    normal_transactions.reset_index(inplace=True, drop=True)

    normal_transactions["isScam"] = [
        1 if k in scam_tokens else 0 for k in normal_transactions["contractAddress"]
    ]
    normal_transactions = normal_transactions[normal_transactions["isScam"] == 0]

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
    normal_transactions.index = normal_transactions["timeStamp"].map(
        lambda x: dt.datetime.fromtimestamp(int(x))
    )

    normal_transactions.rename(
        columns={"from": "From", "to": "To", "value": "From Amount", "gas": "Fee"},
        inplace=True,
    )
    normal_transactions.drop(
        [
            "blockHash",
            "blockNumber",
            "nonce",
            "transactionIndex",
            "gasPrice",
            "isError",
            "txreceipt_status",
            "contractAddress",
            "cumulativeGasUsed",
            "gasUsed",
            "confirmations",
            "timeStamp",
            "input",
            "isScam",
            "functionName",
            "methodId",
        ],
        axis=1,
        inplace=True,
    )

    normal_transactions["Fiat Price"] = None
    normal_transactions["Fiat"] = "EUR"
    normal_transactions["Notes"] = ""
    normal_transactions["Source"] = f"ETH-{address[0:5]}"
    normal_transactions["Fee Coin"] = "ETH"
    normal_transactions["To Coin"] = None
    normal_transactions["From Coin"] = "ETH"
    normal_transactions["Fee Fiat"] = None
    normal_transactions["To Amount"] = None
    normal_transactions["Tag"] = "Movement"

    normal_transactions.sort_index(inplace=True)

    outdf = normal_transactions.copy()

    # INTERNAL
    url = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey=GFID2HN2QCS6UR4K1CX13F946P2V1S7Q7X"
    response_internal = requests.get(url)
    internal_transactions1 = pd.DataFrame(response_internal.json().get("result"))
    internal_transactions = internal_transactions1.copy()
    if internal_transactions.shape[0] > 0:
        internal_transactions.index = internal_transactions["timeStamp"].map(
            lambda x: dt.datetime.fromtimestamp(int(x))
        )

        normal_transactions_bis = pd.DataFrame(response.json().get("result"))
        normal_transactions_bis.index = normal_transactions_bis["timeStamp"].map(
            lambda x: dt.datetime.fromtimestamp(int(x))
        )

        internal_transactions = internal_transactions.join(
            normal_transactions_bis["gasPrice"], how="left"
        )
        internal_transactions.bfill(inplace=True)

        internal_transactions = internal_transactions[
            internal_transactions["isError"] != 1
        ].copy()

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
        internal_transactions["gas"] = [
            -(
                int(internal_transactions.loc[i, "gas"])
                * int(internal_transactions.loc[i, "gasPrice"])
            )
            / 10**18
            for i in range(internal_transactions.shape[0])
        ]
        internal_transactions.index = internal_transactions["timeStamp"].map(
            lambda x: dt.datetime.fromtimestamp(int(x))
        )

        internal_transactions.rename(
            columns={"from": "From", "to": "To", "value": "From Amount", "gas": "Fee"},
            inplace=True,
        )
        internal_transactions.drop(
            [
                "blockNumber",
                "gasPrice",
                "isError",
                "traceId",
                "gasUsed",
                "timeStamp",
                "input",
                "contractAddress",
                "type",
                "errCode",
            ],
            axis=1,
            inplace=True,
        )

        internal_transactions["Fiat Price"] = None
        internal_transactions["Fiat"] = "EUR"
        internal_transactions["Notes"] = ""
        internal_transactions["Source"] = f"ETH-{address[0:5]}"
        internal_transactions["Fee Coin"] = "ETH"
        internal_transactions["To Coin"] = None
        internal_transactions["From Coin"] = "ETH"
        internal_transactions["Fee Fiat"] = None
        internal_transactions["To Amount"] = None
        internal_transactions["Tag"] = "Movement"

        sub1 = internal_transactions.loc[
            internal_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ]
        sub2 = internal_transactions.loc[
            internal_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ]
        internal_transactions.loc[
            internal_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ] = sub1.values
        internal_transactions.loc[
            internal_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ] = sub2.values

        outdf = pd.concat([outdf, internal_transactions], axis=0)
    outdf.sort_index(inplace=True)

    outdf.loc[outdf["From Amount"] == "", "From Coin"] = None
    outdf.loc[outdf["From Amount"] == "", "From Amount"] = None
    outdf.loc[outdf["To Amount"] == "", "To Coin"] = None
    outdf.loc[outdf["To Amount"] == "", "To Amount"] = None

    outdf.loc[outdf["To Amount"] > 0, "Fee"] = 0
    outdf.loc[outdf["To Amount"] > 0, "Fee Coin"] = None

    sub1 = outdf.loc[outdf["From Amount"] > 0, ["From Amount", "From Coin"]]
    sub2 = outdf.loc[outdf["From Amount"] > 0, ["To Amount", "To Coin"]]
    outdf.loc[outdf["From Amount"] > 0, ["To Amount", "To Coin"]] = sub1.values
    outdf.loc[outdf["From Amount"] > 0, ["From Amount", "From Coin"]] = sub2.values

    # Get ERC20 tokens
    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey=GFID2HN2QCS6UR4K1CX13F946P2V1S7Q7X"
    response = requests.get(url)
    erc20_transactions1 = pd.DataFrame(response.json().get("result"))
    erc20_transactions = erc20_transactions1.copy()
    if erc20_transactions.shape[0] > 0:
        erc20_transactions["from"] = erc20_transactions["from"].map(lambda x: x.lower())
        erc20_transactions["to"] = erc20_transactions["to"].map(lambda x: x.lower())

        erc20_transactions["isScam"] = [
            1 if k in scam_tokens else 0 for k in erc20_transactions["contractAddress"]
        ]
        erc20_transactions = erc20_transactions[erc20_transactions["isScam"] == 0]

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

        erc20_transactions.rename(
            columns={
                "from": "From",
                "to": "To",
                "value": "From Amount",
                "gas": "Fee",
                "tokenSymbol": "From Coin",
            },
            inplace=True,
        )
        erc20_transactions.index = erc20_transactions["timeStamp"].map(
            lambda x: dt.datetime.fromtimestamp(int(x))
        )
        erc20_transactions.drop(
            [
                "blockNumber",
                "timeStamp",
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
                "isScam",
            ],
            axis=1,
            inplace=True,
        )

        erc20_transactions.loc[
            erc20_transactions["From"] == address.lower(), "From Amount"
        ] *= -1

        erc20_transactions["Fiat Price"] = None
        erc20_transactions["Fiat"] = "EUR"
        erc20_transactions["Fee Coin"] = "ETH"
        erc20_transactions["Fee Fiat"] = None
        erc20_transactions["Fee"] = None
        erc20_transactions["To Amount"] = None
        erc20_transactions["Tag"] = "Movement"
        erc20_transactions["To Coin"] = None
        erc20_transactions["Notes"] = ""
        erc20_transactions["Source"] = f"ETH-{address[0:5]}"

        sub1 = erc20_transactions.loc[
            erc20_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ]
        sub2 = erc20_transactions.loc[
            erc20_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ]
        erc20_transactions.loc[
            erc20_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ] = sub1.values
        erc20_transactions.loc[
            erc20_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ] = sub2.values

        outdf = pd.concat([outdf, erc20_transactions])
        outdf.sort_index(inplace=True)

    outdf["Fee"].fillna(0, inplace=True)

    outdf = outdf[
        [
            "hash",
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

    # UNISWAP SWAPS
    if internal_transactions.shape[0] > 0 and erc20_transactions.shape[0] > 0:
        erc_internal = pd.merge(
            internal_transactions,
            erc20_transactions,
            on="hash",
            suffixes=("-INT", "-ERC"),
        )
        from_am_uniswap = erc_internal["From Amount-INT"].combine_first(
            erc_internal["From Amount-ERC"]
        )
        to_am_uniswap = erc_internal["To Amount-INT"].combine_first(
            erc_internal["To Amount-ERC"]
        )
        from_uniswap = erc_internal["From Coin-INT"].combine_first(
            erc_internal["From Coin-ERC"]
        )
        to_uniswap = erc_internal["To Coin-INT"].combine_first(
            erc_internal["From Coin-ERC"]
        )
        erc_internal["From Amount-INT"] = from_am_uniswap
        erc_internal["To Amount-INT"] = to_am_uniswap
        erc_internal["From Coin-INT"] = from_uniswap
        erc_internal["To Coin-INT"] = to_uniswap

        new_index = list(set(outdf[outdf["hash"].isin(erc_internal["hash"])].index))
        erc_internal = erc_internal[
            [x for x in erc_internal.columns if "-INT" in x or x == "hash"]
        ]
        outdf = outdf[~outdf["hash"].isin(erc_internal["hash"])]
        erc_internal = erc_internal[
            [
                "From-INT",
                "To-INT",
                "From Coin-INT",
                "To Coin-INT",
                "From Amount-INT",
                "To Amount-INT",
                "Fee-INT",
                "Fee Coin-INT",
                "Fee Fiat-INT",
                "Fiat-INT",
                "Fiat Price-INT",
                "Tag-INT",
                "Source-INT",
                "Notes-INT",
            ]
        ]

        outdf.drop(["hash"], axis=1, inplace=True)

        erc_internal.columns = outdf.columns
        erc_internal.index = new_index
        erc_internal["Notes"] = "Uniswap"
        erc_internal["Tag"] = "Trade"

        outdf = pd.concat([outdf, erc_internal], sort=True)
        outdf.sort_index(inplace=True)

    outdf = tx.price_transactions_df(outdf, Prices())

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
