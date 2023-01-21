import os
import pandas as pd
import tax_library as tx


def get_transactions_df(raw=False):
    coinbase_files = [
        os.path.join(os.getcwd(), "coinbase", x)
        for x in os.listdir(os.path.join(os.getcwd(), "coinbase"))
    ]

    if len(coinbase_files) == 0:
        print("No files for coinbase found")
        return None
    else:
        df_list = []
        for filename in coinbase_files:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)
        final_df = final_df[final_df["Transaction Type"] != "Learning Reward"]

        final_df.index = [
            tx.str_to_datetime(j.replace("T", " ").replace("Z", ""))
            for j in final_df["Timestamp"]
        ]

        final_df.sort_index(inplace=True)
        final_df = final_df.drop_duplicates()
        final_df.rename(columns={"Fees and/or Spread": "Fees"}, inplace=True)

        if raw:
            return final_df

        final_df["From"] = ""
        final_df["To"] = ""
        final_df["Fee Coin"] = "EUR"
        final_df["To Coin"] = ""
        final_df["To Amount"] = ""
        final_df["Source"] = "Coinbase"

        final_df.rename(
            columns={
                "Transaction Type": "Tag",
                "Asset": "From Coin",
                "Quantity Transacted": "From Amount",
                "Spot Price at Transaction": "Fiat Price",
                "Spot Price Currency": "Fiat",
                "Fees": "Fee",
            },
            inplace=True,
        )

        # Convert transactions
        final_df.loc[final_df["Tag"] == "Convert", "From Amount"] *= -1

        final_df.loc[final_df["Tag"] == "Convert", "To Coin"] = [
            i.split(" ")[-1]
            for i in list(final_df.loc[final_df["Tag"] == "Convert", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Convert", "To Amount"] = [
            float(i.split(" ")[-2].replace(".", "").replace(",", "."))
            for i in list(final_df.loc[final_df["Tag"] == "Convert", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Convert", "Tag"] = "Trade"

        # Receive transactions
        final_df.loc[final_df["Tag"] == "Receive", "To Amount"] = final_df.loc[
            final_df["Tag"] == "Receive", "From Amount"
        ]
        final_df.loc[final_df["Tag"] == "Receive", "To Coin"] = final_df.loc[
            final_df["Tag"] == "Receive", "From Coin"
        ]

        final_df.loc[final_df["Tag"] == "Receive", "From Amount"] = None
        final_df.loc[final_df["Tag"] == "Receive", "From Coin"] = None

        final_df.loc[final_df["Tag"] == "Receive", "From"] = [
            i.split(" ")[-1]
            for i in list(final_df.loc[final_df["Tag"] == "Receive", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Receive", "Tag"] = "Movement"

        # Buy transactions
        final_df.loc[final_df["Tag"] == "Buy", "To Amount"] = final_df.loc[
            final_df["Tag"] == "Buy", "From Amount"
        ]
        final_df.loc[final_df["Tag"] == "Buy", "To Coin"] = final_df.loc[
            final_df["Tag"] == "Buy", "From Coin"
        ]

        final_df.loc[final_df["Tag"] == "Buy", "From Coin"] = [
            i.split(" ")[-1]
            for i in list(final_df.loc[final_df["Tag"] == "Buy", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Buy", "From Amount"] = [
            -float(i.split(" ")[4].replace(",", "."))
            for i in list(final_df.loc[final_df["Tag"] == "Buy", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Buy", "Tag"] = "Trade"

        # Sell transactions
        final_df.loc[final_df["Tag"] == "Sell", "From Amount"] *= -1
        final_df.loc[final_df["Tag"] == "Sell", "To Coin"] = [
            i.split(" ")[-1]
            for i in list(final_df.loc[final_df["Tag"] == "Sell", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Sell", "To Amount"] = [
            float(i.split(" ")[4].replace(",", "."))
            for i in list(final_df.loc[final_df["Tag"] == "Sell", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Sell", "Tag"] = "Trade"

        # Send Transactions
        final_df.loc[final_df["Tag"] == "Send", "From Amount"] *= -1
        final_df.loc[final_df["Tag"] == "Send", "Fee Coin"] = None

        final_df.loc[final_df["Tag"] == "Send", "To"] = [
            i.split(" ")[-1]
            for i in list(final_df.loc[final_df["Tag"] == "Send", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Send", "Tag"] = "Movement"

        # Coinbase Earn transactions
        final_df.loc[final_df["Tag"] == "Coinbase Earn", "To Amount"] = final_df.loc[
            final_df["Tag"] == "Coinbase Earn", "From Amount"
        ]
        final_df.loc[final_df["Tag"] == "Coinbase Earn", "To Coin"] = final_df.loc[
            final_df["Tag"] == "Coinbase Earn", "From Coin"
        ]

        final_df.loc[final_df["Tag"] == "Coinbase Earn", "From Amount"] = None
        final_df.loc[final_df["Tag"] == "Coinbase Earn", "From Coin"] = None

        final_df.loc[final_df["Tag"] == "Coinbase Earn", "From"] = [
            i.split(" ")[-1]
            for i in list(final_df.loc[final_df["Tag"] == "Coinbase Earn", "Notes"])
        ]

        final_df.loc[final_df["Tag"] == "Coinbase Earn", "Tag"] = "Reward"

        # -------------------

        final_df["Fiat Price"] = final_df["Subtotal"]
        final_df.drop(
            [
                "Timestamp",
                "Subtotal",
                "Total (inclusive of fees and/or spread)",
                "Notes",
            ],
            axis=1,
            inplace=True,
        )

        final_df = final_df.reindex(
            columns=[
                "From",
                "To",
                "From Coin",
                "To Coin",
                "From Amount",
                "To Amount",
                "Fee",
                "Fee Coin",
                "Fiat",
                "Fiat Price",
                "Tag",
                "Source",
                "Fee Fiat",
            ]
        )

        final_df.loc[final_df["From"] == "XYO", "Tag"] = "COIN"
        final_df.loc[final_df["Tag"] == "COIN", "Tag"] = "Reward"
        final_df.loc[final_df["From"] == "COIN", "Tag"] = "Reward"
        final_df["Notes"] = ""

        final_df["Fee Fiat"] = final_df["Fee"]

        return final_df


def get_eur_invested(year=None):
    all_trx = get_transactions_df()
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    return -all_trx.loc[all_trx["From Coin"] == "EUR", "From Amount"].sum()
