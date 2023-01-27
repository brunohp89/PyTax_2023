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

# ONLY AUTO CAKE SYRUP POOL IS SUPPORTED FOR PANCAKE SWAP
def get_transactions_df(address, beacon_address=None):
    with open(os.getcwd() + "\\.json") as creds:
        api_key = json.load(creds)["BSCScanToken"]

    if api_key == "":
        raise PermissionError("No API KEY for BSC Scan found in .json")

    address = address.lower()

    if beacon_address is not None:
        beacon = tx.get_bnb(beacon_address)

    url = f"https://api.bscscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999999&page=1&offset=1000&sort=asc&apikey={api_key}"
    response = requests.get(url)

    normal_transactions = pd.DataFrame(response.json().get("result"))
    normal_transactions = normal_transactions[
        normal_transactions["isError"] != 1
    ].copy()
    normal_transactions.reset_index(inplace=True, drop=True)

    normal_transactions["isScam"] = [
        1 if k in scam_tokens else 0 for k in normal_transactions["from"]
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
            "isScam",
            "cumulativeGasUsed",
            "gasUsed",
            "confirmations",
            "timeStamp",
            "input",
            "methodId",
            "functionName",
        ],
        axis=1,
        inplace=True,
    )

    normal_transactions["Fiat Price"] = None
    normal_transactions["Fiat"] = "EUR"
    normal_transactions["Notes"] = ""
    normal_transactions["Source"] = f"BSC-{address[0:5]}"
    normal_transactions["Fee Coin"] = "BNB"
    normal_transactions["To Coin"] = None
    normal_transactions["From Coin"] = "BNB"
    normal_transactions["Fee Fiat"] = None
    normal_transactions["To Amount"] = None
    normal_transactions["Tag"] = "Movement"

    normal_transactions.sort_index(inplace=True)

    if beacon_address is not None:
        outdf = pd.concat([beacon, normal_transactions], axis=0)
    else:
        outdf = normal_transactions.copy()
    outdf.sort_index(inplace=True)

    sub1 = outdf.loc[outdf["From Amount"] > 0, ["From Amount", "From Coin"]]
    sub2 = outdf.loc[outdf["From Amount"] > 0, ["To Amount", "To Coin"]]
    outdf.loc[outdf["From Amount"] > 0, ["To Amount", "To Coin"]] = sub1.values
    outdf.loc[outdf["From Amount"] > 0, ["From Amount", "From Coin"]] = sub2.values

    # INTERNAL
    url = f"https://api.bscscan.com/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=99999999999&page=1&offset=10&sort=asc&apikey=AD3549Z3D6T3J24SPSY3SZRMD9CUAANS91"
    response_internal = requests.get(url)
    internal_transactions = pd.DataFrame(response_internal.json().get("result"))

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

        internal_transactions["isScam"] = [
            1 if k in scam_tokens else 0 for k in internal_transactions["from"]
        ]
        internal_transactions = internal_transactions[
            internal_transactions["isScam"] == 0
        ]
        internal_transactions.reset_index(inplace=True, drop=True)

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
                "isScam",
                "type",
                "errCode",
            ],
            axis=1,
            inplace=True,
        )

        internal_transactions["Fiat Price"] = None
        internal_transactions["Fiat"] = "EUR"
        internal_transactions["Notes"] = ""
        internal_transactions["Source"] = f"BSC-{address[0:5]}"
        internal_transactions["Fee Coin"] = "BNB"
        internal_transactions["To Coin"] = None
        internal_transactions["From Coin"] = "BNB"
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

    # outdf = pd.concat([outdf, internal_transactions], axis=0)
    outdf.sort_index(inplace=True)

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

    outdf.loc[
        outdf["To"] == "0x0000000000000000000000000000000000001004", "To Amount"
    ] *= 0

    # Get BEP20 tokens
    url = f"https://api.bscscan.com/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999999999&sort=asc&apikey=AD3549Z3D6T3J24SPSY3SZRMD9CUAANS91"
    response = requests.get(url)
    bep20_transactions = pd.DataFrame(response.json().get("result"))
    if bep20_transactions.shape[0] > 0:
        bep20_transactions["from"] = bep20_transactions["from"].map(lambda x: x.lower())
        bep20_transactions["to"] = bep20_transactions["to"].map(lambda x: x.lower())

        bep20_transactions["isScam"] = [
            1 if k in scam_tokens else 0 for k in bep20_transactions["from"]
        ]
        bep20_transactions = bep20_transactions[bep20_transactions["isScam"] == 0]
        bep20_transactions.reset_index(inplace=True, drop=True)

        bep20_transactions["value"] = [
            int(s) / 10 ** int(x)
            for s, x in zip(
                bep20_transactions["value"], bep20_transactions["tokenDecimal"]
            )
        ]
        bep20_transactions["gas"] = [
            -(
                int(bep20_transactions.loc[i, "gasUsed"])
                * int(bep20_transactions.loc[i, "gasPrice"])
            )
            / 10**18
            for i in range(bep20_transactions.shape[0])
        ]

        bep20_transactions.rename(
            columns={
                "from": "From",
                "to": "To",
                "value": "From Amount",
                "gas": "Fee",
                "tokenSymbol": "From Coin",
            },
            inplace=True,
        )
        bep20_transactions.index = bep20_transactions["timeStamp"].map(
            lambda x: dt.datetime.fromtimestamp(int(x))
        )
        bep20_transactions.drop(
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
                "isScam",
                "input",
                "tokenName",
            ],
            axis=1,
            inplace=True,
        )

        bep20_transactions.loc[
            bep20_transactions["From"] == address.lower(), "From Amount"
        ] *= -1

        bep20_transactions.loc[bep20_transactions["To"] == address, "Fee"] = 0

        bep20_transactions["Fiat Price"] = None
        bep20_transactions["Fiat"] = "EUR"
        bep20_transactions["Fee Coin"] = "BNB"
        bep20_transactions["Fee Fiat"] = None
        bep20_transactions["Fee"] = None
        bep20_transactions["To Amount"] = None
        bep20_transactions["Tag"] = "Movement"
        bep20_transactions["To Coin"] = None
        bep20_transactions["Notes"] = ""
        bep20_transactions["Source"] = f"BSC-{address[0:5]}"

        sub1 = bep20_transactions.loc[
            bep20_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ]
        sub2 = bep20_transactions.loc[
            bep20_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ]
        bep20_transactions.loc[
            bep20_transactions["From Amount"] > 0, ["To Amount", "To Coin"]
        ] = sub1.values
        bep20_transactions.loc[
            bep20_transactions["From Amount"] > 0, ["From Amount", "From Coin"]
        ] = sub2.values

        outdf = pd.concat([outdf, bep20_transactions])
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

    # PANCAKE SWAP SWAPS
    pancake_df = outdf.loc[
        outdf[outdf["To"] == "0x10ed43c718714eb63d5aa57b78b54704e256024e"].index, :
    ]
    outdf = outdf[~outdf["hash"].isin(pancake_df["hash"])]
    for hash in set(pancake_df["hash"]):
        temp_df = pancake_df[pancake_df["hash"] == hash]
        fee = temp_df["Fee"].sum()
        temp_df = temp_df[
            ~np.logical_and(
                temp_df["From Amount"].isin([None, 0]),
                temp_df["To Amount"].isin([None, 0]),
            )
        ]
        temp_df["Fee"] = fee
        temp_df.ffill(inplace=True)
        temp_df.bfill(inplace=True)
        temp_df["Notes"] = "PancakeSwap"
        temp_df["Tag"] = "Trade"
        outdf = pd.concat([outdf, temp_df.iloc[[0], :]])

    outdf.sort_index(inplace=True)

    # PANCAKE SWAP CAKE SYRUP POOL
    pancake_df = outdf.loc[
        outdf[
            np.logical_or(
                outdf["To"].isin(
                    [
                        "0xa80240eb5d7e05d3f250cf000eec0891d00b51cc",
                        "0x45c54210128a065de780c4b0df3d16664f7f859e",
                    ]
                ),
                outdf["From"].isin(
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
        outdf = outdf[~outdf["hash"].isin(pancake_df["hash"])]
        profit = pancake_df[["From Amount", "To Amount"]].sum().sum()

        pancake_df = pancake_df[
            np.logical_and(
                pancake_df["To Coin"] != "Cake", pancake_df["From Coin"] != "Cake"
            )
        ]
        if profit > 0:
            profit_trx = pancake_df.iloc[[0], :].copy()
            profit_trx.loc[:, ["Fee", "From Coin", "From Amount", "Fee Coin"]] = None
            profit_trx["Tag"] = "Reward"
            profit_trx["Notes"] = "Pancake Syrup Pool"
            profit_trx["To Coin"] = "Cake"
            profit_trx["To Amount"] = profit
            outdf = pd.concat([outdf, profit_trx])
        outdf = pd.concat([outdf, pancake_df])

    # STARGATE LP - REMOVE S* Tokens
    outdf = outdf[~outdf["From Coin"].str.contains("\*", regex=True, na=False)]
    outdf = outdf[~outdf["To Coin"].str.contains("\*", regex=True, na=False)]

    # REMOVE S*BUSD - ADD MANUALLY EACH TOKEN YOU USE IN STARGATE
    outdf = outdf[outdf["To"] != "0x98a5737749490856b401db5dc27f522fc314a4e1"]
    outdf = outdf[outdf["From"] != "0x98a5737749490856b401db5dc27f522fc314a4e1"]

    # STARGATE VESTING

    outdf = outdf[outdf["To"] != "0xd4888870c8686c748232719051b677791dbda26d"]

    # STARGATE LP REWARDS
    stargate = outdf.loc[
        np.logical_or(
            outdf["From"] == "0x3052a0f6ab15b4ae1df39962d5ddefaca86dab47",
            outdf["To"] == "0x3052a0f6ab15b4ae1df39962d5ddefaca86dab47",
        )
    ]
    if stargate.shape[0] > 0:
        outdf = outdf[~outdf["hash"].isin(stargate["hash"])]
        stargate.loc[
            np.logical_and(stargate["To Amount"] > 0, stargate["To Coin"] == "STG"),
            "Tag",
        ] = "Reward"
        stargate.loc[
            np.logical_and(stargate["To Amount"] > 0, stargate["To Coin"] == "STG"),
            "Notes",
        ] = "Stargate"

        outdf = pd.concat([outdf, stargate])

    outdf.sort_index(inplace=True)

    outdf = tx.price_transactions_df(outdf, Prices())

    return outdf
