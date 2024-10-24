import numpy as np
import pandas as pd
import datetime as dt
import tax_library as tx
from PricesClass import Prices
from blockfrost import BlockFrostApi
from utils import date_from_timestamp
import os

def get_transactions_df(address, blockfrost_api):

    api = BlockFrostApi(project_id=blockfrost_api)
    stake_address = api.address(address)
    transactions = []
    addresses = api.account_addresses(stake_address.stake_address)
    addresses = [k.address for k in addresses]

    for address in addresses:
        transactions.extend(api.address_transactions(address))

    trx_content, trx_utxos = [], []
    for transaction in transactions:
        trx_content.append(api.transaction(transaction.tx_hash))
        trx_utxos.append(api.transaction_utxos(transaction.tx_hash))

    # outputs per transactions
    output, inputs, in_address, out_address = [], [], [], []
    for utxos in trx_utxos:
        for outputs in utxos.outputs:
            for tok in outputs.amount:
                output.append((utxos.hash, outputs.address, tok.unit, tok.quantity))
    for utxos in trx_utxos:
        for input in utxos.inputs:
            for tok in input.amount:
                inputs.append((utxos.hash, input.address, tok.unit, tok.quantity))

    output = pd.DataFrame(output)
    inputs = pd.DataFrame(inputs)
    timestamp = pd.DataFrame(
        [(k.hash, date_from_timestamp(k.block_time)) for k in trx_content]
    )
    fees = pd.DataFrame([(k.hash, int(k.fees)) for k in trx_content])

    vout = pd.merge(timestamp, fees, on=0, suffixes=("1_", "2_"))
    vout.drop_duplicates(inplace=True)
    vout = pd.merge(vout, inputs, on=0, suffixes=("3_", "4_"))
    vout.drop_duplicates(inplace=True)
    vout = pd.merge(vout, output, on=0, suffixes=("5_", "6_"))
    vout.drop_duplicates(inplace=True)

    vout.columns = [
        "Tx Hash",
        "Timestamp",
        "Fees",
        "IAddress",
        "IAsset",
        "IAmount",
        "OAddress",
        "OAsset",
        "OAmount",
    ]
    vout = vout[
        np.logical_or(
            vout["OAddress"].isin(addresses), vout["IAddress"].isin(addresses)
        )
    ]

    vout["IAmount"] = vout["IAmount"].apply(lambda x: int(x))
    vout["OAmount"] = vout["OAmount"].apply(lambda x: int(x))
    vout["Fees"] = vout["Fees"].apply(lambda x: -int(x))

    vout = pd.concat(
        [
            vout,
            vout.loc[
                np.logical_and(
                    vout["IAddress"].isin(addresses), vout["OAddress"].isin(addresses)
                )
            ],
        ]
    ).drop_duplicates(keep=False)

    vout.drop_duplicates(
        subset=["Timestamp", "OAddress", "OAsset", "OAmount"], inplace=True
    )

    vout.loc[~vout["OAddress"].isin(addresses), "OAmount"] *= -1

    vout.drop_duplicates(subset=["Timestamp", "OAsset", "OAmount"], inplace=True)

    vout.loc[vout["OAsset"] == "lovelace", "OAsset"] = "ADA"
    vout.loc[vout["IAsset"] == "lovelace", "IAsset"] = "ADA"

    ada = vout[vout["OAsset"] == "ADA"]
    ada = ada[ada["IAsset"] == ada["OAsset"]]
    ada.drop_duplicates(subset=["Tx Hash", "Fees"], inplace=True, keep="last")
    ada.loc[ada["OAmount"] > 0, "Fees"] *= 0

    rewards = api.account_rewards(stake_address.stake_address)
    rewards = [
        (date_from_timestamp(api.epoch(k.epoch).end_time), int(k.amount))
        for k in rewards
    ]
    rewards = pd.DataFrame(rewards)
    if rewards.shape[0] > 0:
        delegate_transaction = ada.iloc[[-1], :].copy()
        delegate_transaction["OAmount"] = -2176149
        delegate_transaction["Fees"] = None
        delegate_transaction["Timestamp"] += dt.timedelta(seconds=1)
        ada = pd.concat([delegate_transaction, ada])
    rewards.columns = ["Timestamp", "OAmount"]
    ada = pd.concat([ada, rewards])
    ada.index = ada["Timestamp"]
    ada["Fees"] /= 10**6
    ada["OAmount"] /= 10**6
    new_fees = (
        ada[["Tx Hash", "Fees"]].groupby("Tx Hash").mean()
        / ada[["Tx Hash", "Fees"]].groupby("Tx Hash").count()
    )
    new_fees["Tx Hash"] = new_fees.index
    new_fees.reset_index(drop=True, inplace=True)
    ada = pd.merge(ada, new_fees, on="Tx Hash", how="outer")

    assets = set(vout.loc[vout["OAsset"] != "ADA", "OAsset"])
    symbols = []
    for symbol in assets:
        response = api.asset(symbol)
        if response.onchain_metadata is not None:
            if len(response.onchain_metadata.name) > 5:
                symbols.append((symbol, response.onchain_metadata.files[0].name))
            else:
                symbols.append((symbol, response.onchain_metadata.name))
        else:
            symbols.append((symbol, response.metadata.ticker))
    symbols = pd.DataFrame(symbols)

    if symbols.shape[0] > 0:
        symbols.columns = ["OAsset", "Ticker"]
        vout = pd.merge(vout, symbols, on="OAsset", how="outer")
        vout.index = vout["Timestamp"]
        vout.sort_index(inplace=True)

        assets = vout[~pd.isna(vout["Ticker"])]
        assets = assets[assets["IAsset"] != assets["OAsset"]]
        assets.drop_duplicates(subset=["Ticker", "OAmount"], inplace=True)
        assets = pd.concat(
            [
                assets[~assets["IAddress"].isin(addresses)],
                assets[~assets["OAddress"].isin(addresses)],
            ]
        )
        assets.loc[assets["OAddress"].isin(addresses), "Fees"] *= 0
        assets["Fees"] /= 10**6
        assets["OAmount"] /= 10**6
        new_fees = (
            assets[["Tx Hash", "Fees"]].groupby("Tx Hash").mean()
            / assets[["Tx Hash", "Fees"]].groupby("Tx Hash").count()
        )
        new_fees["Tx Hash"] = new_fees.index
        new_fees.reset_index(drop=True, inplace=True)
        assets = pd.merge(assets, new_fees, on="Tx Hash")

        vout = pd.concat([ada, assets])
        vout["Ticker"].fillna("ADA", inplace=True)
    else:
        vout = ada.copy()
        vout["Ticker"] = "ADA"
        vout = vout[
            [
                "Tx Hash",
                "Timestamp",
                "Fees_x",
                "IAddress",
                "IAsset",
                "IAmount",
                "OAddress",
                "OAsset",
                "OAmount",
                "Ticker",
                "Fees_y",
            ]
        ]

    vout.index = vout["Timestamp"]
    vout.sort_index(inplace=True)
    vout.drop(
        ["Tx Hash", "Timestamp", "Fees_x", "IAsset", "IAmount", "OAsset"],
        axis=1,
        inplace=True,
    )

    vout.rename(
        columns={
            "IAddress": "From",
            "OAddress": "To",
            "OAmount": "From Amount",
            "Ticker": "From Coin",
            "Fees_y": "Fee",
        },
        inplace=True,
    )

    vout["Fee Fiat"] = None
    vout["To Coin"] = None
    vout["To Amount"] = None
    vout["Fee Coin"] = "ADA"
    vout["Tag"] = "Movement"
    vout["Notes"] = ""
    vout["Fiat"] = "EUR"
    vout["Fiat Price"] = None
    vout["Notes"] = ""
    vout["Source"] = f"ADA-{stake_address.stake_address[6:11]}"

    vout.loc[pd.isna(vout["From"]), "From"] = None
    vout.loc[pd.isna(vout["To"]), "To"] = None

    sub1 = vout.loc[vout["From Amount"] > 0, ["From Amount", "From Coin"]]
    sub2 = vout.loc[vout["From Amount"] > 0, ["To Amount", "To Coin"]]
    vout.loc[vout["From Amount"] > 0, ["To Amount", "To Coin"]] = sub1.values
    vout.loc[vout["From Amount"] > 0, ["From Amount", "From Coin"]] = sub2.values

    vout.loc[pd.isna(vout["From"]), "Tag"] = "Reward"

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

    vout = vout[vout["To Coin"] != "NEWM"]
    vout = vout[vout["From Coin"] != "NEWM"]

    if "cardano.csv" in os.listdir(os.path.join('input')):
        manual = pd.read_csv("input/cardano.csv", parse_dates=True, index_col="Timestamp")
        vout = pd.concat([manual, vout])

    vout = tx.price_transactions_df(vout, Prices())

    return vout
