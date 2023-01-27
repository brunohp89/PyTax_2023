import numpy as np
from PricesClass import Prices
import tax_library as tx
import datetime as dt
import pandas as pd
import psycopg2


conn = psycopg2.connect(
    host="104.199.89.51",
    database="mainnet_explorer",
    user="public_readonly",
    password="nearprotocol",
)
cur = conn.cursor()


def create_pandas_table(sql_query, database=conn):
    table = pd.read_sql_query(sql_query, database)
    return table


def get_transactions_df(addresses_list):
    addlist = addresses_list
    addresses = "','".join(addlist)
    addresses = f"'{addresses}'"

    query_normal = f"""
    SELECT block_timestamp as "timestamp", signer_account_id as "From", receiver_account_id as "To", action_kind as "kind", "args"
    FROM TRANSACTIONS inner join transaction_actions ac on transactions.transaction_hash = ac.transaction_hash
    WHERE signer_account_id in ({addresses})
	OR receiver_account_id in ({addresses})
	AND action_kind = 'TRANSFER'"""

    normal_transactions = create_pandas_table(query_normal)

    normal_transactions = normal_transactions[normal_transactions["kind"] == "TRANSFER"]
    normal_transactions["kind"] = "Movement"

    normal_transactions["From"] = normal_transactions["From"].map(lambda x: x.lower())
    normal_transactions["To"] = normal_transactions["To"].map(lambda x: x.lower())

    normal_transactions.index = normal_transactions["timestamp"].map(
        lambda x: dt.datetime.fromtimestamp(int(x) / 10**9)
    )

    normal_transactions["args"] = normal_transactions["args"].map(
        lambda x: int(x["deposit"]) / 10**24
    )

    normal_transactions.rename(
        columns={"args": "From Amount", "kind": "Tag"}, inplace=True
    )
    normal_transactions.drop(["timestamp"], axis=1, inplace=True)

    normal_transactions["Fiat Price"] = None
    normal_transactions["Fiat"] = "EUR"
    normal_transactions["Fee Coin"] = "NEAR"
    normal_transactions["Fee"] = None
    normal_transactions["To Coin"] = None
    normal_transactions["From Coin"] = "NEAR"
    normal_transactions["To Amount"] = None
    normal_transactions["Fee Fiat"] = None
    normal_transactions["Tag"] = "Movement"
    normal_transactions["Notes"] = None
    normal_transactions["Source"] = "NEAR"

    normal_transactions.loc[
        np.logical_and(
            normal_transactions["From"].isin(addlist),
            ~normal_transactions["To"].isin(addlist),
        ),
        "From Amount",
    ] *= -1

    # Get NEP20 tokens
    query_nep20 = f"""SELECT INCLUDED_IN_BLOCK_TIMESTAMP AS "timestamp",
	AMOUNT,
	EVENT_KIND,
	TOKEN_NEW_OWNER_ACCOUNT_ID AS "To",
	TOKEN_OLD_OWNER_ACCOUNT_ID AS "From",
	RECEIPT_CONVERSION_GAS_BURNT AS "gas",
	upper(substring(emitted_by_contract_account_id,7)) as "coin"
    FROM ASSETS__FUNGIBLE_TOKEN_EVENTS AC
    INNER JOIN RECEIPTS RP ON AC.EMITTED_FOR_RECEIPT_ID = RP.RECEIPT_ID
    INNER JOIN TRANSACTIONS TRX ON TRX.TRANSACTION_HASH = RP.ORIGINATED_FROM_TRANSACTION_HASH
    WHERE TOKEN_NEW_OWNER_ACCOUNT_ID in ({addresses})
	OR TOKEN_OLD_OWNER_ACCOUNT_ID in ({addresses})"""
    nep20_transactions = create_pandas_table(query_nep20)

    if nep20_transactions.shape[0] > 0:
        nep20_transactions["From"] = nep20_transactions["From"].map(lambda x: x.lower())
        nep20_transactions["To"] = nep20_transactions["To"].map(lambda x: x.lower())

        nep20_transactions.loc[
            nep20_transactions["To"] == "deposits.grow.sweat".lower(), "amount"
        ] = 0  # Sweat staking

        nep20_transactions["amount"] = [
            int(s) / 10**18 for s in nep20_transactions["amount"]
        ]
        nep20_transactions["gas"] = [
            int(s) / 10**18 for s in nep20_transactions["gas"]
        ]

        nep20_transactions.loc[
            np.logical_and(
                nep20_transactions["To"].isin(addlist),
                nep20_transactions["From"].isin(addlist),
                nep20_transactions["event_kind"] == "TRANSFER",
            ),
            "amount",
        ] = 0

        nep20_transactions.rename(
            columns={
                "from": "From",
                "to": "To",
                "amount": "From Amount",
                "gas": "Fee",
                "coin": "From Coin",
                "event_kind": "Tag",
            },
            inplace=True,
        )
        nep20_transactions.index = nep20_transactions["timestamp"].map(
            lambda x: dt.datetime.fromtimestamp(int(x) / 10**9)
        )
        nep20_transactions.drop(["timestamp"], axis=1, inplace=True)

        nep20_transactions.loc[
            np.logical_and(
                nep20_transactions["From"].isin(addlist),
                ~nep20_transactions["To"].isin(addlist),
                nep20_transactions["Tag"] == "TRANSFER",
            ),
            "From Amount",
        ] *= -1

        nep20_transactions["Fiat Price"] = None
        nep20_transactions["Fiat"] = "EUR"
        nep20_transactions["Fee Coin"] = "NEAR"
        nep20_transactions["To Amount"] = None
        nep20_transactions["To Coin"] = None
        nep20_transactions.loc[
            nep20_transactions["Tag"] == "TRANSFER", "Tag"
        ] = "Movement"
        nep20_transactions.loc[nep20_transactions["Tag"] == "MINT", "Tag"] = "Reward"
        nep20_transactions.loc[
            np.logical_and(
                nep20_transactions["Tag"] == "Movement",
                nep20_transactions["From"] == "tge-lockup.sweat",
            ),
            "Tag",
        ] = "Reward"  # SWEAT airdrops
        nep20_transactions["Fee Fiat"] = None
        nep20_transactions["Notes"] = None
        nep20_transactions["Source"] = "NEAR"

        outdf = pd.concat([nep20_transactions, normal_transactions])
    else:
        outdf = normal_transactions.copy()

    outdf.sort_index(inplace=True)

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
    outdf["Fee"] *= -1
    sub1 = outdf.loc[outdf["From Amount"] > 0, ["From Amount", "From Coin"]]
    sub2 = outdf.loc[outdf["From Amount"] > 0, ["To Amount", "To Coin"]]
    outdf.loc[outdf["From Amount"] > 0, ["To Amount", "To Coin"]] = sub1.values
    outdf.loc[outdf["From Amount"] > 0, ["From Amount", "From Coin"]] = sub2.values

    outdf = tx.price_transactions_df(outdf, Prices())
    return outdf
