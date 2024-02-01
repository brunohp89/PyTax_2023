import os
import numpy as np
import pandas as pd
import tax_library as tx
from PricesClass import Prices


def get_transactions_df(raw=False, return_fiat=False):
    # Because SUPERCHARGER is paid in multiple transactions with the same timestamp, if
    # we have more than one file with duplicated transactions in the crypto.com folder
    # the amount of supercharger will be wrong
    if return_fiat:
        fiat_files =  [
        os.path.join(os.path.abspath('crypto.com'), x)
        for x in os.listdir(os.path.abspath('crypto.com'))
            if "fiat" in x
        ]

        if len(fiat_files) == 0:
            print("No fiat for crypto.com found")
        else:
            df_list = []
            for filename in fiat_files:
                df_loop = pd.read_csv(filename, index_col=None, header=0)
                df_list.append(df_loop)
            fiat_df = pd.concat(df_list, axis=0, ignore_index=True)
            fiat_df.index = [tx.str_to_datetime(j) for j in fiat_df["Timestamp (UTC)"]]

            fiat_df.drop_duplicates(inplace=True)
            fiat_df.sort_index(inplace=True)
            return fiat_df

    cdc_files =  [
        os.path.join(os.path.abspath('crypto.com'), x)
        for x in os.listdir(os.path.abspath('crypto.com'))
        if "crypto_transactions_record" in x
    ]
    if len(cdc_files) == 0:
        print("No files for crypto.com found")
        return None
    else:
        df_list = []
        for filename in cdc_files:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)

        final_df.loc[final_df["Currency"] == "LUNA2", "Currency"] = "LUNA"
        final_df.loc[final_df["To Currency"] == "LUNA2", "Currency"] = "LUNA"

        super_charger = final_df[final_df['Transaction Description'] == 'Supercharger Reward']
        final_df = final_df[final_df['Transaction Description'] != 'Supercharger Reward']

        final_df.drop_duplicates(
            inplace=True,
            subset=["Timestamp (UTC)", "Amount", "Transaction Description", "Currency"],
        )

        final_df = pd.concat([final_df,super_charger])

        fix = final_df[
            final_df["Transaction Kind"]
            == "trading.limit_order.crypto_wallet.fund_unlock"
        ]
        fix = fix.iloc[[0], :]
        fix["Timestamp (UTC)"] = "2022-07-18 17:12:53"
        fix["Transaction Description"] = "Withdraw USDC (ETH)"
        fix["Currency"] = "USDC"
        fix["Amount"] = -34.92794
        fix["To Currency"] = fix["To Amount"] = None
        fix["Native Currency"] = "USD"
        fix["Native Amount"] = 34.92794
        fix["Native Amount (in USD)"] = 34.92794

        final_df = pd.concat([final_df, fix])

        final_df.index = [tx.str_to_datetime(j) for j in final_df["Timestamp (UTC)"]]
        final_df.sort_index(inplace=True)

        if raw:
            return final_df

        final_df["From"] = None
        final_df["To"] = None
        final_df["Fee"] = None
        final_df["Fee Coin"] = None
        final_df.rename(
            columns={
                "Transaction Kind": "Tag",
                "Currency": "From Coin",
                "To Currency": "To Coin",
                "Native Amount": "Fiat Price",
                "Native Currency": "Fiat",
                "Amount": "From Amount",
            },
            inplace=True,
        )

        final_df.loc[
            final_df["Transaction Description"].str.contains("Recurring"),
            "Transaction Description",
        ] = "Recurring"
        final_df.loc[
            final_df["Transaction Description"].str.contains("Buy"), "To Coin"
        ] = "EUR"
        final_df.loc[
            final_df["Transaction Description"].str.contains("Buy"), "To Amount"
        ] = final_df.loc[
            final_df["Transaction Description"].str.contains("Buy"), "From Amount"
        ]

        final_df = final_df[
            ~final_df["Transaction Description"].isin(
                [
                    "Crypto Earn Deposit",
                    "Crypto Earn Allocation",
                    "Crypto Earn Withdrawal",
                    "CRO Stake",
                    "CRO Unstake",
                    "CRO Lockup",
                    "CRO Unlock",
                    "Supercharger Lockup (via app)",
                    "Supercharger Deposit (via app)",
                    "Supercharger Stake (via app)",
                    "Supercharger Withdrawal (via app)",
                    "Cardholder CRO Stake"

                ]
            )
        ]

        final_df.loc[
            final_df["Transaction Description"] == "Recurring", "From Amount"
        ] *= -1

        final_df["Notes"] = ""

        final_df.loc[
            final_df["Transaction Description"].isin(
                ["Card Cashback", "Card Rebate: Spotify", "Card Cashback Reversal"]
            ),
            "Tag",
        ] = "Reward"

        final_df.loc[
            final_df["Transaction Description"].isin(
                ["Card Cashback", "Card Rebate: Spotify", "Card Cashback Reversal"]
            ),
            "Notes",
        ] = "Cashback"

        final_df.loc[
            final_df["Tag"].str.contains("earn|supercharger"), "Tag"
        ] = "Interest"
        final_df.loc[
            final_df["Transaction Description"].str.contains("Reward"), "Tag"
        ] = "Reward"
        final_df.loc[~pd.isna(final_df["To Amount"]), "Tag"] = "Trade"
        final_df.loc[
            ~final_df["Tag"].isin(["Cashback", "Interest", "Reward", "Trade"]), "Tag"
        ] = "Movement"

        final_df.drop(
            ["Timestamp (UTC)", "Transaction Description", "Native Amount (in USD)"],
            axis=1,
            inplace=True,
        )

        final_df.loc[
            np.logical_and(final_df["From"] == "", final_df["From Amount"] < 0), "From"
        ] = "Crypto.com"
        final_df["Source"] = "Crypto.com"

        # Aggiunta fee che non è nello storico
        final_df.loc[final_df.index == "2022-08-08 18:21:40", "Fee"] = -0.00335
        final_df.loc[final_df.index == "2022-08-08 18:21:40", "Fee Coin"] = "BTC"

        sub1 = final_df.loc[final_df["From Amount"] > 0, ["From Amount", "From Coin"]]
        sub2 = final_df.loc[final_df["From Amount"] > 0, ["To Amount", "To Coin"]]
        final_df.loc[
            final_df["From Amount"] > 0, ["To Amount", "To Coin"]
        ] = sub1.values
        final_df.loc[
            final_df["From Amount"] > 0, ["From Amount", "From Coin"]
        ] = sub2.values

        final_df["Fee Fiat"] = None
        final_df = tx.price_transactions_df(final_df, Prices())
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

        final_df.loc[final_df["Tag"] == 'Trade', 'From Amount'] = [-abs(c) for c in
                                                               final_df.loc[final_df["Tag"] == 'Trade', 'From Amount']]

        return final_df


def get_eur_invested(year=None):
    all_trx = get_transactions_df(return_fiat=True)
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    return all_trx.loc[all_trx["Transaction Kind"] == "viban_purchase", "Amount"].sum() + all_trx.loc[all_trx["Transaction Kind"] == "recurring_buy_order", "Amount"].sum() - all_trx.loc[all_trx["Transaction Kind"] == "crypto_viban", "Amount"].sum()
