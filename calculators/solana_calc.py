from PricesClass import Prices
import datetime as dt
import pandas as pd
import requests
import tax_library as tx
import os
import numpy as np
from solana.rpc.api import Client, Signature
import json
import time
import pickle as pk

nfts_to_remove = [
    "SEED1",
    "WATER",
    "WOOD",
    "HABITAT",
    "FIRE",
    "IRON",
    "WATErpZ2ZBjgAxyttoEjckuTuCe9pEckSabCeENLTYq",
    "woodN5KSiHEAhaCrZVh3vScGta7u6r5Vp3UbqDFuD4e",
    "seeD1wGXYWjio2dcok5DyYKDoVfeVgMASoi7azfyrr4",
    "9eYpMvyRMJUeGD77pYdhdNZxnuwBiusGhSohbV98jFaS",
]


def get_transactions_df(address):
    solapi = Client("https://api.mainnet-beta.solana.com")

    with open(os.path.join(os.getcwd(), ".json")) as creds:
        apikey = json.load(creds)["SolScanAPIToken"]

    if apikey == "":
        raise PermissionError("No API KEY for SolScan found in .json")

    response = requests.get(
        f"https://public-api.solscan.io/account/exportTransactions?account={address}&type=all&fromTime=1611839871&toTime={int(dt.datetime.now().timestamp())}",
        headers={"accept": "application/json", "token": f"{apikey}"},
    )
    with open("temp.csv", "wb") as handle:
        handle.write(response.content)
    response_pd = pd.read_csv("temp.csv")
    response_pd.columns = [k.strip() for k in response_pd.columns]
    os.remove("temp.csv")

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

    # TOKENS TRANSACTIONS

    tokens_transactions = response_pd[response_pd["Type"] == "TokenChange"].copy()

    tokens_transactions.loc[
        tokens_transactions["TokenName(off-chain)"].str.contains("Habitat"),
        "Symbol(off-chain)",
    ] = "HABITAT"

    fees_to_distribute = tokens_transactions.loc[
        ~tokens_transactions["Symbol(off-chain)"].isin(nfts_to_remove), "Fee (SOL)"
    ].sum()

    tokens_transactions = tokens_transactions[
        ~tokens_transactions["Symbol(off-chain)"].isin(nfts_to_remove)
    ]

    tokens_transactions = tokens_transactions[
        ~pd.isna(tokens_transactions["Symbol(off-chain)"])
    ]

    if f"{address[0:6]}_tokens.pickle" in os.listdir():
        with open(f"{address[0:6]}_tokens.pickle", "rb") as handle:
            tx_contents = pk.load(handle)
        tx_contents.columns = ["Txhash", "Txcontent"]
        tx_contents["Txcontentstr"] = tx_contents["Txcontent"].apply(lambda x: str(x))
        tx_contents = tx_contents.drop_duplicates(subset=["Txhash", "Txcontentstr"])
        tx_contents = tx_contents.drop("Txcontentstr", axis=1)
        tokens_transactions = pd.merge(
            tokens_transactions, tx_contents, on="Txhash", how="left"
        )

    if "Txcontent" not in tokens_transactions.columns:
        tx_to_download = tokens_transactions["Txhash"].unique()
        tx_contents = []
    else:
        tx_to_download = tokens_transactions.loc[
            pd.isna(tokens_transactions["Txcontent"]), "Txhash"
        ].unique()
        tx_contents = [
            [x, y]
            for x, y in zip(
                tokens_transactions.loc[
                    ~pd.isna(tokens_transactions["Txcontent"]), "Txhash"
                ],
                tokens_transactions.loc[
                    ~pd.isna(tokens_transactions["Txcontent"]), "Txcontent"
                ],
            )
        ]

    if len(tx_to_download) > 0:
        print(
            f"{len(tx_to_download)} new token transactions identified, it will take around {len(tx_to_download) * 5}s to complete the download"
        )
        for hash in tx_to_download:
            tx_contents.append(
                [
                    hash,
                    json.loads(
                        solapi.get_transaction(Signature.from_string(hash)).to_json()
                    ),
                ]
            )
            time.sleep(5)
        tx_contents = pd.concat([pd.DataFrame(x).T for x in tx_contents])
        tx_contents.columns = ["Txhash", "Txcontent"]
        tokens_transactions = pd.merge(tokens_transactions, tx_contents, on="Txhash")

        with open(f"{address[0:6]}_tokens.pickle", "wb") as handle:
            pk.dump(tx_contents, handle, pk.HIGHEST_PROTOCOL)

    tokens_transactions["Tag"] = ""
    tokens_transactions["Notes"] = ""
    tokens_transactions["From"] = ""
    tokens_transactions["To"] = ""
    tokens_transactions["From Amount"] = None
    tokens_transactions["From Coin"] = None
    tokens_transactions["To Amount"] = None
    tokens_transactions["To Coin"] = None

    tokens_transactions["Fee (SOL)"] *= -1
    tokens_transactions.index = [
        dt.datetime.fromtimestamp(pd.Timestamp(k).timestamp())
        for k in tokens_transactions["BlockTime"]
    ]

    tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] > 0, "To Amount"
    ] = tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] > 0, "SPL BalanceChange"
    ]
    tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] < 0, "From Amount"
    ] = tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] < 0, "SPL BalanceChange"
    ]
    tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] > 0, "To Coin"
    ] = tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] > 0, "Symbol(off-chain)"
    ]
    tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] < 0, "From Coin"
    ] = tokens_transactions.loc[
        tokens_transactions["SPL BalanceChange"] < 0, "Symbol(off-chain)"
    ]

    tokens_transactions = tokens_transactions.drop_duplicates(subset=["Txhash"])
    if tokens_transactions.shape[0] > 0:
        for hash in tokens_transactions["Txhash"].unique():
            tx_content = list(
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "Txcontent"
                ]
            )[0]

            zerobal = pd.DataFrame(
                data=np.zeros((1, 6)),
                columns=[
                    "accountIndex",
                    "mint",
                    "uiTokenAmount",
                    "owner",
                    "programId",
                    "type",
                ],
            )

            pre_balances = pd.DataFrame(
                tx_content["result"]["meta"]["preTokenBalances"]
            )
            post_balances = pd.DataFrame(
                tx_content["result"]["meta"]["postTokenBalances"]
            )

            if pre_balances.shape[0] == 0:
                pre_balances = zerobal.copy()
                pre_balances["mint"] = post_balances["mint"].tolist()[0]
            else:
                pre_balances["type"] = "pre"
                pre_balances["uiTokenAmount"] = pre_balances["uiTokenAmount"].apply(
                    lambda x: float(x["uiAmountString"])
                )

            if post_balances.shape[0] == 0:
                post_balances = zerobal.copy()
                post_balances["mint"] = pre_balances["mint"].tolist()[0]
            else:
                post_balances["type"] = "post"
                post_balances["uiTokenAmount"] = post_balances["uiTokenAmount"].apply(
                    lambda x: float(x["uiAmountString"])
                )

            txpd = pd.merge(
                pre_balances, post_balances, on="mint", suffixes=("-pre", "-post")
            )

            txpd = txpd[~txpd["mint"].isin(nfts_to_remove)]
            txpd["change"] = txpd["uiTokenAmount-post"] - txpd["uiTokenAmount-pre"]

            txpd = txpd[
                np.logical_or(
                    txpd["owner-pre"] == address, txpd["owner-post"] == address
                )
            ]
            txpd.loc[txpd["owner-post"] != txpd["owner-pre"], "change"] = txpd.loc[
                txpd["owner-post"] != txpd["owner-pre"], "uiTokenAmount-post"
            ]

            txpd = txpd.loc[txpd["owner-post"] == address]

            txpd = txpd[txpd["change"] != 0]
            txpd.drop_duplicates(
                inplace=True, subset=["accountIndex-pre", "mint", "uiTokenAmount-pre"]
            )
            txpd.drop_duplicates(
                inplace=True, subset=["accountIndex-post", "mint", "change"]
            )

            dup_tok = txpd.groupby("mint").agg({"owner-post": "count"}).reset_index()
            dup_tok = dup_tok[dup_tok["owner-post"] > 1]

            if dup_tok.shape[0] > 0:
                for mint in dup_tok["mint"].unique():
                    if (
                        txpd[
                            np.logical_and(
                                txpd["owner-post"] == txpd["owner-pre"],
                                txpd["mint"] == mint,
                            )
                        ].shape[0]
                        > 0
                    ):
                        txpd = txpd.drop(
                            txpd[
                                np.logical_and(
                                    txpd["owner-post"] != txpd["owner-pre"],
                                    txpd["mint"] == mint,
                                )
                            ].index,
                            axis=0,
                        )

            txpd = txpd.groupby("mint").agg({"change": "sum"}).reset_index()

            if txpd.shape[0] == 1 and "Instruction: Transfer," in ",".join(
                tx_content["result"]["meta"]["logMessages"]
            ):
                from_amount = txpd.loc[txpd["change"] < 0, "change"].tolist()
                if len(from_amount) == 0:
                    from_amount = [None]
                from_coin = txpd.loc[txpd["change"] < 0, "mint"].tolist()
                if len(from_coin) == 0:
                    from_coin = [None]
                to_amount = txpd.loc[txpd["change"] > 0, "change"].tolist()
                if len(to_amount) == 0:
                    to_amount = [None]
                to_coin = txpd.loc[txpd["change"] > 0, "mint"].tolist()
                if len(to_coin) == 0:
                    to_coin = [None]
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "From Amount"
                ] = from_amount[0]
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "From Coin"
                ] = from_coin[0]
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "To Amount"
                ] = to_amount[0]
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "To Coin"
                ] = to_coin[0]

                if "STEPNq2UGeGSzCyGVr2nMQAzf8xuejwqebd84wcksCK" in pre_balances["owner"].tolist():
                    tokens_transactions.loc[tokens_transactions["Txhash"] == hash,"Tag"] = "Reward"
                    tokens_transactions.loc[tokens_transactions["Txhash"] == hash,"Notes"] = "STEPN"
                    continue
            if txpd.shape[0] > 2:
                if (
                    "Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j"
                    in ",".join(tx_content["result"]["meta"]["logMessages"])
                    and txpd.shape[0] == 3
                ):
                    tempdf = pd.concat(
                        [
                            tokens_transactions.loc[
                                tokens_transactions["Txhash"] == hash
                            ].copy(),
                            tokens_transactions.loc[
                                tokens_transactions["Txhash"] == hash
                            ].copy(),
                        ]
                    )
                    from_amount = txpd.loc[txpd["change"] < 0, "change"].tolist()
                    if len(from_amount) == 1:
                        from_amount.append(None)
                    from_coin = txpd.loc[txpd["change"] < 0, "mint"].tolist()
                    if len(from_coin) == 1:
                        from_coin.append(None)
                    to_amount = txpd.loc[txpd["change"] > 0, "change"].tolist()
                    if len(to_amount) == 1:
                        to_amount.append(None)
                    to_coin = txpd.loc[txpd["change"] > 0, "mint"].tolist()
                    if len(to_coin) == 1:
                        to_coin.append(None)
                    tempdf["From Coin"] = from_coin
                    tempdf["From Amount"] = from_amount
                    tempdf["To Coin"] = to_coin
                    tempdf["To Amount"] = to_amount

                    tokens_transactions = tokens_transactions[
                        tokens_transactions["Txhash"] != hash
                    ]
                    tokens_transactions = pd.concat([tokens_transactions, tempdf])
                    continue
                else:
                    print(f"WARNING, problem with transaction {hash}")
                    continue

            if txpd.loc[txpd["change"] < 0, "change"].shape[0] > 0:
                if txpd.loc[txpd["change"] < 0, "change"].shape[0] == 1:
                    from_amount = float(txpd.loc[txpd["change"] < 0, "change"])
                    from_coin = list(txpd.loc[txpd["change"] < 0, "mint"])[0]
                elif txpd.loc[txpd["change"] < 0, "change"].shape[0] > 1:
                    from_amount = [
                        float(x) for x in txpd.loc[txpd["change"] < 0, "change"]
                    ]
                    from_coin = [
                        str(x) for x in list(txpd.loc[txpd["change"] < 0, "mint"])
                    ]
                else:
                    from_amount = float(0)
                    from_coin = None
            else:
                from_amount = float(0)
                from_coin = None

            if txpd.loc[txpd["change"] > 0, "change"].shape[0] > 0:
                if txpd.loc[txpd["change"] > 0, "change"].shape[0] == 1:
                    to_amount = float(txpd.loc[txpd["change"] > 0, "change"])
                    to_coin = list(txpd.loc[txpd["change"] > 0, "mint"])[0]
                elif txpd.loc[txpd["change"] > 0, "change"].shape[0] > 1:
                    to_amount = [
                        float(x) for x in txpd.loc[txpd["change"] > 0, "change"]
                    ]
                    to_coin = [
                        str(x) for x in list(txpd.loc[txpd["change"] > 0, "mint"])
                    ]
                else:
                    to_amount = float(0)
                    to_coin = None
            else:
                to_amount = float(0)
                to_coin = None

            if (
                tx_content["result"]["meta"]["postBalances"][0]
                - tx_content["result"]["meta"]["preBalances"][0]
                + tx_content["result"]["meta"]["fee"]
            ) < 0:
                if isinstance(from_amount, list):
                    from_coin.append("SOL")
                    from_amount.append(
                        (
                            tx_content["result"]["meta"]["postBalances"][0]
                            - tx_content["result"]["meta"]["preBalances"][0]
                        )
                        / 10**9
                    )
                else:
                    if "RefineCrystals" in ",".join(
                        tx_content["result"]["meta"]["logMessages"]
                    ):
                        from_coin = [from_coin, "SOL"]
                        from_amount = [
                            from_amount,
                            (
                                tx_content["result"]["meta"]["postBalances"][0]
                                - tx_content["result"]["meta"]["preBalances"][0]
                            )
                            / 10**9,
                        ]
                        tokens_transactions.loc[
                            tokens_transactions["Txhash"] == hash, "Notes"
                        ] = "Genopets"
                    else:
                        from_coin = "SOL"
                        from_amount = (
                            tx_content["result"]["meta"]["postBalances"][0]
                            - tx_content["result"]["meta"]["preBalances"][0]
                        ) / 10**9

            if (
                tx_content["result"]["meta"]["postBalances"][0]
                - tx_content["result"]["meta"]["preBalances"][0]
                + tx_content["result"]["meta"]["fee"]
            ) > 0:

                if isinstance(to_amount, list):
                    to_coin.append("SOL")
                    to_amount.append(
                        (
                            tx_content["result"]["meta"]["postBalances"][0]
                            - tx_content["result"]["meta"]["preBalances"][0]
                        )
                        / 10**9
                    )
                else:
                    if "WithdrawKi" in ",".join(
                        tx_content["result"]["meta"]["logMessages"]
                    ):
                        to_coin = [to_coin, "SOL"]
                        to_amount = [
                            to_amount,
                            (
                                tx_content["result"]["meta"]["postBalances"][0]
                                - tx_content["result"]["meta"]["preBalances"][0]
                            )
                            / 10**9,
                        ]
                        tokens_transactions.loc[
                            tokens_transactions["Txhash"] == hash, "Tag"
                        ] = "Reward"
                        tokens_transactions.loc[
                            tokens_transactions["Txhash"] == hash, "Notes"
                        ] = "Genopets"
                    else:
                        to_coin = "SOL"
                        to_amount = (
                            tx_content["result"]["meta"]["postBalances"][0]
                            - tx_content["result"]["meta"]["preBalances"][0]
                        ) / 10**9

            if isinstance(from_amount, float):
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "From Amount"
                ] = from_amount
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "From Coin"
                ] = from_coin
            else:
                tempdf = tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash
                ].copy()
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "From Amount"
                ] = from_amount[0]
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "From Coin"
                ] = from_coin[0]
                for i, x in enumerate(from_amount[1:]):
                    tempdf["From Amount"] = x
                    tempdf["From Coin"] = from_coin[i + 1]
                    tokens_transactions = pd.concat([tokens_transactions, tempdf])

            if isinstance(to_amount, float):
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "To Amount"
                ] = to_amount
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "To Coin"
                ] = to_coin
            else:
                tempdf = tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash
                ].copy()
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "To Amount"
                ] = to_amount[0]
                tokens_transactions.loc[
                    tokens_transactions["Txhash"] == hash, "To Coin"
                ] = to_coin[0]
                for i, x in enumerate(to_amount[1:]):
                    tempdf["To Amount"] = x
                    tempdf["To Coin"] = to_coin[i + 1]
                    tokens_transactions = pd.concat([tokens_transactions, tempdf])

        mapping_df = tokens_transactions[
            ["TokenAddress", "Symbol(off-chain)"]
        ].drop_duplicates()
        mapping_token = {
            x: y
            for x, y in zip(mapping_df["TokenAddress"], mapping_df["Symbol(off-chain)"])
        }
        mapping_token["SOL"] = "SOL"

        tokens_transactions["From Coin"] = tokens_transactions["From Coin"].map(
            mapping_token
        )
        tokens_transactions["To Coin"] = tokens_transactions["To Coin"].map(
            mapping_token
        )

        tokens_transactions["From Amount"] = tokens_transactions["From Amount"].apply(
            lambda x: -abs(x) if x is not None else x
        )

        tokens_transactions = tokens_transactions.rename(columns={"Fee (SOL)": "Fee"})
        tokens_transactions = tokens_transactions.drop(
            [
                "Txhash",
                "BlockTime Unix",
                "BlockTime",
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
                "Type",
            ],
            axis=1,
        )

        tokens_transactions["From"] = tokens_transactions["To"] = None
        tokens_transactions["Fee Coin"] = "SOL"
        tokens_transactions["Fiat Price"] = None
        tokens_transactions["Fiat"] = "EUR"

        tokens_transactions["Fee"] -= fees_to_distribute / tokens_transactions.shape[0]

        vout = pd.concat([normal_transactions, tokens_transactions])
        vout = vout.sort_index()
    else:
        vout = normal_transactions.copy()

    vout = tx.price_transactions_df(vout, Prices())
    vout["Source"] = f"Solana-{address[0:4]}"
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

    dup_index = vout.index[vout.index.duplicated()]

    for dupindex in dup_index:
        if (
            len(set(vout.loc[dupindex, "From Amount"]))
            and list(set(vout.loc[dupindex, "From Coin"]))[0] == "SOL"
        ):
            vout.loc[dupindex, "From Amount"] /= vout.loc[
                dupindex, "From Amount"
            ].shape[0]
        if (
            len(set(vout.loc[dupindex, "To Amount"]))
            and list(set(vout.loc[dupindex, "To Coin"]))[0] == "SOL"
        ):
            vout.loc[dupindex, "To Amount"] /= vout.loc[dupindex, "To Amount"].shape[0]

    vout = vout.sort_index()
    vout.loc[np.logical_and(vout["To Coin"] == 'GMT', vout['Tag']=='Reward'), "Tag"] = "Movement"
    vout.loc[np.logical_and(vout["From Coin"] == 'GMT', vout['Tag'] == 'Reward'), "Tag"] = "Movement"

    return vout
