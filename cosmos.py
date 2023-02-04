import numpy as np
from PricesClass import Prices
import tax_library as tx
import datetime as dt
import pandas as pd
import requests


def get_transactions_df(address: str):
    # convert the address to lowercase
    address = address.lower()
    # format the API URL
    api_url = f"https://api.cosmoscan.net/transactions?address={address}"
    # make a GET request to retrieve transactions data
    response = requests.get(api_url)
    # create a dataframe from the response JSON
    transactions = pd.DataFrame(response.json()["items"])
    # retrieve transaction details for each transaction hash
    trx_details = transactions["hash"].apply(
        lambda hash: requests.get(f"https://api.cosmoscan.net/transaction/{hash}")
    )
    trx_details = trx_details.apply(lambda response: pd.DataFrame(response.json()))
    # concatenate all the transaction details into a single dataframe
    normal_transactions = pd.concat(list(trx_details), axis=0, ignore_index=True)
    # initialize an empty list to store the final transaction dataframes
    final_df_list = []
    # loop through each transaction to process it
    for index in range(normal_transactions.shape[0]):
        trx = normal_transactions.iloc[index]
        if trx["messages"]["type"] == "Delegate":
            prior_trx = normal_transactions.iloc[index - 1]
            if prior_trx["messages"]["type"] == "WithdrawDelegatorReward":
                final_df_list.append(
                    pd.DataFrame(
                        [
                            [
                                # convert timestamp to datetime
                                dt.datetime.fromtimestamp(int(trx["created_at"])),
                                trx["messages"]["body"]["delegator_address"],
                                trx["messages"]["body"]["validator_address"],
                                int(trx["messages"]["body"]["amount"]["amount"])
                                / 10**6,
                                trx["messages"]["body"]["amount"]["denom"][1:].upper(),
                                # negate the transaction fee
                                -float(trx["fee"]),
                                "ATOM",
                                "Reward",
                                "EUR",
                                0,
                                f"Cosmos-{address[6:11]}",
                                "",
                            ]
                        ]
                    )
                )
            else:
                final_df_list.append(
                    pd.DataFrame(
                        [
                            [
                                # convert timestamp to datetime
                                dt.datetime.fromtimestamp(int(trx["created_at"])),
                                trx["messages"]["body"]["delegator_address"],
                                trx["messages"]["body"]["validator_address"],
                                0,
                                trx["messages"]["body"]["amount"]["denom"][1:].upper(),
                                # negate the transaction fee
                                -float(trx["fee"]),
                                "ATOM",
                                "Delegate",
                                "EUR",
                                0,
                                f"Cosmos-{address[6:11]}",
                                "",
                            ]
                        ]
                    )
                )
        elif trx["messages"]["type"] == "Send":
            final_df_list.append(
                pd.DataFrame(
                    [
                        [
                            # convert timestamp to datetime
                            dt.datetime.fromtimestamp(int(trx["created_at"])),
                            trx["messages"]["body"]["from_address"],
                            trx["messages"]["body"]["to_address"],
                            int(trx["messages"]["body"]["amount"][0]["amount"])
                            / 10**6,
                            trx["messages"]["body"]["amount"][0]["denom"][1:].upper(),
                            # negate the transaction fee
                            -float(trx["fee"]),
                            "ATOM",
                            "Movement",
                            "EUR",
                            0,
                            f"Cosmos-{address[6:11]}",
                            "",
                        ]
                    ]
                )
            )
        elif trx["messages"]["type"] == "WithdrawDelegatorReward":
            final_df_list.append(
                pd.DataFrame(
                    [
                        [
                            # convert timestamp to datetime
                            dt.datetime.fromtimestamp(int(trx["created_at"])),
                            trx["messages"]["body"]["validator_address"],
                            trx["messages"]["body"]["delegator_address"],
                            # set the amount to zero
                            0,
                            "ATOM",
                            # negate the transaction fee
                            -float(trx["fee"]),
                            "ATOM",
                            "Movement",
                            "EUR",
                            0,
                            f"Cosmos-{address[6:11]}",
                            "Delegate rewards",
                        ]
                    ]
                )
            )
    # concatenate the final transaction dataframes into a single dataframe
    final_df = pd.concat(final_df_list, ignore_index=True, axis=0)

    final_df.columns = [
        "Timestamp",
        "From",
        "To",
        "From Amount",
        "From Coin",
        "Fee",
        "Fee Coin",
        "Tag",
        "Fiat",
        "Fiat Price",
        "Source",
        "Notes",
    ]
    final_df.index = final_df["Timestamp"]
    final_df.drop(["Timestamp"], inplace=True, axis=1)

    final_df["To Coin"] = None
    final_df["To Amount"] = None
    final_df["Fee Fiat"] = None

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

    final_df.sort_index(inplace=True)
    final_df.loc[
        np.logical_and(final_df["From"] == address, final_df["Tag"] != "Reward"),
        "From Amount",
    ] *= -1

    final_df = tx.price_transactions_df(final_df, Prices())

    return final_df
