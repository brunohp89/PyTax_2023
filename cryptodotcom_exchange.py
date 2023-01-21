# Esportare depositi, ORDERS, supercharge, withdrawal in CSV. Rimuovere le prime due righe e cancellare file vuoti
# PROBLEMI: Non vengono gestiti casi di duplicati per ora
# PROBLEMI: Se non ci sono tutti: WITHDRAWALS; DEPOSITS; SPOT_ORDER potrebbero mancare delle colonne
import os
import numpy as np
import pandas as pd
import tax_library as tx
from PricesClass import Prices


def get_transactions_df(raw=False, update_prices=True):
    cdc_files = [
        os.path.join(os.getcwd(), "crypto.com exchange", x)
        for x in os.listdir(os.path.join(os.getcwd(), "crypto.com exchange"))
    ]

    if len(cdc_files) == 0:
        print("No files for crypto.com exchange found")
        return None
    else:
        df_list = []
        for filename in cdc_files:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)

        if raw:
            return final_df

        final_df = final_df[final_df["Status"] != "CANCELED"].copy()

        final_df["From"] = None
        final_df["Tag"] = None
        final_df["To"] = None
        final_df["To Coin"] = None
        final_df["To Amount"] = None
        final_df["Withdrawal Amount"] *= -1

        final_df.loc[~pd.isna(final_df["Amount"]), "Tag"] = "Interest"

        final_df.loc[~pd.isna(final_df["Deposit Address"]), "From"] = final_df.loc[
            ~pd.isna(final_df["Deposit Address"]), "Deposit Address"
        ]

        final_df.loc[~pd.isna(final_df["Withdrawal Address"]), "To"] = final_df.loc[
            ~pd.isna(final_df["Withdrawal Address"]), "Withdrawal Address"
        ]

        final_df.loc[~pd.isna(final_df["Symbol"]), "Coin"] = final_df.loc[
            ~pd.isna(final_df["Symbol"]), "Symbol"
        ].map(lambda x: x.split("_")[0])

        final_df.loc[~pd.isna(final_df["Symbol"]), "To Coin"] = final_df.loc[
            ~pd.isna(final_df["Symbol"]), "Symbol"
        ].map(lambda x: x.split("_")[1])

        final_df.loc[
            np.logical_and(~pd.isna(final_df["Symbol"]), final_df["Side"] == "SELL"),
            "Trade Amount",
        ] *= -1
        final_df.loc[
            np.logical_and(~pd.isna(final_df["Symbol"]), final_df["Side"] == "BUY"),
            "Volume of Business",
        ] *= -1

        final_df.loc[~pd.isna(final_df["Deposit Amount"]), "Amount"] = final_df.loc[
            ~pd.isna(final_df["Deposit Amount"]), "Deposit Amount"
        ]

        final_df.loc[~pd.isna(final_df["Trade Amount"]), "Amount"] = final_df.loc[
            ~pd.isna(final_df["Trade Amount"]), "Trade Amount"
        ]
        final_df.loc[
            ~pd.isna(final_df["Volume of Business"]), "To Amount"
        ] = final_df.loc[~pd.isna(final_df["Volume of Business"]), "Volume of Business"]

        final_df.loc[~pd.isna(final_df["Withdrawal Amount"]), "Amount"] = final_df.loc[
            ~pd.isna(final_df["Withdrawal Amount"]), "Withdrawal Amount"
        ]

        final_df.index = [tx.str_to_datetime(j[:-4]) for j in final_df["Time (UTC)"]]

        final_df.drop(
            [
                "Deposit Address",
                "Deposit Amount",
                "Status",
                "TxId",
                "Trade ID",
                "Symbol",
                "Trade Price",
                "Order ID",
                "Side",
                "Trade Amount",
                "Volume of Business",
                "Withdrawal Amount",
                "Withdrawal Address",
                "Txid",
            ],
            axis=1,
            inplace=True,
        )
        final_df["Fee"].fillna(0, inplace=True)
        final_df["To Amount"].fillna(0, inplace=True)

        final_df.drop_duplicates(
            inplace=True,
            subset=[
                "Time (UTC)",
                "Coin",
                "Amount",
                "From",
                "To",
                "To Coin",
                "To Amount",
            ],
        )

        final_df.drop(["Time (UTC)"], axis=1, inplace=True)

        final_df.loc[pd.isna(final_df["Fee Currency"]), "Fee Currency"] = final_df.loc[
            pd.isna(final_df["Fee Currency"]), "Coin"
        ]
        final_df.loc[final_df["Tag"] == "", "Tag"] = "Movement"
        final_df["Fiat"] = "EUR"
        final_df["Fiat Price"] = None
        final_df["Notes"] = ""
        final_df["Fee Fiat"] = None

        final_df.rename(
            columns={
                "Coin": "From Coin",
                "Amount": "From Amount",
                "Fee Currency": "Fee Coin",
            },
            inplace=True,
        )

        final_df = final_df.reindex(
            columns=[
                "From",
                "To",
                "From Coin",
                "From Amount",
                "To Coin",
                "To Amount",
                "Fiat Price",
                "Fiat",
                "Fee",
                "Fee Coin",
                "Tag",
                "Fee Fiat",
                "Notes",
            ]
        )

        final_df.loc[
            np.logical_and(final_df["From"] == "", final_df["From Amount"] < 0), "From"
        ] = "Crypto.com Exchange"
        final_df["Fee"] *= -1

        final_df.sort_index(inplace=True)

        if not update_prices:
            return final_df

        tokens = final_df["From Coin"].tolist()
        tokens.extend(final_df["To Coin"].tolist())
        tokens.extend(final_df["Fee Coin"].tolist())
        tokens = [
            x.upper()
            for x in list(set(tokens))
            if x not in tx.fiat_list and not pd.isna(x)
        ]

        global exch_prices
        exch_prices = Prices()
        exch_prices.get_prices(tokens)
        exch_prices.convert_prices(tokens, "EUR")

        final_df.loc[final_df["From Coin"] == "EUR", "Fiat Price"] = final_df.loc[
            final_df["From Coin"] == "EUR", "From Amount"
        ]
        final_df.loc[final_df["To Coin"] == "EUR", "Fiat Price"] = final_df.loc[
            final_df["To Coin"] == "EUR", "To Amount"
        ]

        for tok in tokens:
            temp_df = final_df[
                np.logical_and(
                    final_df["From Coin"] == tok, pd.isna(final_df["Fiat Price"])
                )
            ]
            if temp_df.shape[0] > 0:
                temp_df.index = [k.date() for k in temp_df.index]
                fiat_prices = pd.merge(
                    exch_prices.prices["Prices"]["EUR"][tok.upper()][
                        ["Open", "Close", "High", "Low"]
                    ],
                    temp_df["From Amount"],
                    how="right",
                    left_index=True,
                    right_index=True,
                )
                fiat_prices = list(
                    fiat_prices.iloc[:, 0:4].mean(axis=1) * fiat_prices["From Amount"]
                )
                final_df.loc[
                    np.logical_and(
                        final_df["From Coin"] == tok, pd.isna(final_df["Fiat Price"])
                    ),
                    "Fiat Price",
                ] = fiat_prices

            temp_df = final_df[
                np.logical_and(
                    final_df["To Coin"] == tok, pd.isna(final_df["Fiat Price"])
                )
            ]
            if temp_df.shape[0] > 0:
                temp_df.index = [k.date() for k in temp_df.index]
                fiat_prices = pd.merge(
                    exch_prices.prices["Prices"]["EUR"][tok.upper()][
                        ["Open", "Close", "High", "Low"]
                    ],
                    temp_df["To Amount"],
                    how="right",
                    left_index=True,
                    right_index=True,
                )
                fiat_prices = list(
                    fiat_prices.iloc[:, 0:4].mean(axis=1) * fiat_prices["To Amount"]
                )
                final_df.loc[
                    np.logical_and(
                        final_df["To Coin"] == tok, pd.isna(final_df["Fiat Price"])
                    ),
                    "Fiat Price",
                ] = fiat_prices

            temp_df = final_df[
                np.logical_and(
                    final_df["Fee Coin"] == tok, pd.isna(final_df["Fee Fiat"])
                )
            ]
            if temp_df.shape[0] > 0:
                temp_df.index = [k.date() for k in temp_df.index]
                fiat_prices = pd.merge(
                    exch_prices.prices["Prices"]["EUR"][tok.upper()][
                        ["Open", "Close", "High", "Low"]
                    ],
                    temp_df["Fee"],
                    how="right",
                    left_index=True,
                    right_index=True,
                )
                fiat_prices = list(
                    fiat_prices.iloc[:, 0:4].mean(axis=1) * fiat_prices["Fee"]
                )
                final_df.loc[
                    np.logical_and(
                        final_df["Fee Coin"] == tok, pd.isna(final_df["Fee Fiat"])
                    ),
                    "Fee Fiat",
                ] = fiat_prices

        sub1 = final_df.loc[final_df["From Amount"] > 0, ["From Amount", "From Coin"]]
        sub2 = final_df.loc[final_df["From Amount"] > 0, ["To Amount", "To Coin"]]
        final_df.loc[
            final_df["From Amount"] > 0, ["To Amount", "To Coin"]
        ] = sub1.values
        final_df.loc[
            final_df["From Amount"] > 0, ["From Amount", "From Coin"]
        ] = sub2.values

        return final_df


def get_eur_invested(year=None):
    all_trx = get_transactions_df(update_prices=False)
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
    return -all_trx.loc[all_trx["From Coin"] == "EUR", "From Amount"].sum()
