import os
from PricesClass import Prices
import numpy as np
import pandas as pd
import tax_library as tx


bin_prices = Prices()


# ACQUISTI FATTI CON LA CARTA DI CREDITO DEVONO ESSERE INSERITE MANUALMENTE NEL DATAFRAME FIX


def get_transactions_df(raw=False, card_transactions=False):
    binance_files = [
        os.path.join(os.getcwd(), "binance", x)
        for x in os.listdir(os.path.join(os.getcwd(), "binance"))
        if "automatico" not in x
    ]

    if len(binance_files) == 0:
        print("No files for binance found")
        return None
    else:
        df_list = []
        for filename in binance_files:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)

        final_df.drop_duplicates(
            subset=["User_ID", "UTC_Time", "Account", "Coin", "Change", "Remark"],
            inplace=True,
        )

        # Acquisti con carta di credito e Rward Center
        fixes = pd.DataFrame(
            {
                "User_ID": [120963140] * 19,
                "UTC_Time": [
                    "2022-05-06 15:28:46",
                    "2022-05-06 15:28:46",
                    "2022-05-06 15:28:46",
                    "2022-11-02 12:42:08",
                    "2022-11-02 12:42:05",
                    "2022-07-14 07:10:03",
                    "2022-08-10 11:57:16",
                    "2022-10-26 17:01:38",
                    "2022-07-22 23:58:37",
                    "2022-05-09 23:27:53",
                    "2022-07-14 06:31:07",
                    "2022-10-23 13:01:11",
                    "2022-08-27 08:51:41",
                    "2022-10-11 12:20:41",
                    "2021-08-16 10:58:52",
                    "2023-01-19 10:58:52",
                    "2022-08-27 09:06:42",
                    "2022-04-12 03:07:40",
                    "2022-12-02 04:07:40",
                ],
                "Account": ["Spot"] * 19,
                "Operation": [
                    "Buy",
                    "Buy",
                    "Fee",
                    "Distribution",
                    "Distribution",
                    "Distribution",
                    "Distribution",
                    "Distribution",
                    "Distribution",
                    "Withdraw",
                    "Withdraw",
                    "Distribution",
                    "Fee",
                    "Withdraw",
                    "Withdraw",
                    "Card Cashback",
                    "Distribution",
                    "Rewards Distribution",
                    "Rewards Distribution",
                ],
                "Coin": [
                    "EUR",
                    "BTC",
                    "BTC",
                    "BURGER",
                    "LDO",
                    "BUSD",
                    "BUSD",
                    "BUSD",
                    "SHIB",
                    "UST",
                    "SHIB",
                    "BUSD",
                    "LUNA",
                    "MATIC",
                    "NEAR",
                    "BNB",
                    "ATOM",
                    "VTHO",
                    "SOL",
                ],
                "Change": [
                    -50,
                    0.00143669,
                    0.00006532,
                    0.5,
                    0.3,
                    -1.18583,
                    0,
                    0,
                    38000,
                    -0.858864,
                    -95762.11,
                    0,
                    -0.03509615,
                    -1.912608,
                    -0.413002,
                    0.0010721899999999174,
                    0.007535,
                    85.097094,
                    0.001379,
                ],
                "Remark": [""] * 19,
            }
        )

        busd_rewards = final_df[
            np.logical_and(
                final_df["Operation"].isin(
                    [
                        "Main and funding account transfer",
                        "Main and Funding Account Transfer",
                    ]
                ),
                final_df["Coin"] == "BUSD",
            )
        ].copy()
        busd_rewards["Operation"] = "Distribution"
        final_df = pd.concat([final_df, fixes, busd_rewards])

        to_exclude = [
            "Launchpad subscribe",
            "POS savings purchase",
            "POS savings redemption",
            "Savings Principal redemption",
            "Savings purchase",
            "Simple Earn Flexible Subscription",
            "Simple Earn Flexible Redemption",
            "Simple Earn Locked Subscription",
            "Simple Earn Locked Redemption",
            "Main and funding account transfer",
            "Staking Purchase",
            "Staking Redemption",
            "Main and Funding Account Transfer",
        ]

        final_df = final_df[~final_df["Operation"].isin(to_exclude)].copy()
        final_df = final_df[final_df["Account"] != "Savings"].copy()

        # DATA FIXES '2022-09-16 15:38:40'
        final_df.loc[
            final_df["UTC_Time"] == "2022-10-11 12:11:43", "UTC_Time"
        ] = "2022-10-11 12:11:42"
        final_df.loc[
            final_df["UTC_Time"] == "2022-09-16 15:38:40", "UTC_Time"
        ] = "2022-09-16 15:38:39"
        final_df.loc[
            final_df["UTC_Time"] == "2022-10-06 17:08:46", "UTC_Time"
        ] = "2022-10-06 17:08:45"
        final_df.loc[
            final_df["UTC_Time"] == "2022-10-20 19:53:06", "UTC_Time"
        ] = "2022-10-20 19:53:05"
        final_df.loc[
            final_df["UTC_Time"] == "2022-10-22 23:40:18", "UTC_Time"
        ] = "2022-10-22 23:40:17"
        final_df.loc[
            final_df["UTC_Time"] == "2021-05-13 08:35:15", "UTC_Time"
        ] = "2021-05-13 08:35:16"
        final_df.loc[
            final_df["UTC_Time"] == "2022-11-09 16:07:26", "UTC_Time"
        ] = "2022-11-09 16:07:25"
        final_df.loc[
            final_df["UTC_Time"] == "2022-07-30 14:19:47", "UTC_Time"
        ] = "2022-07-30 14:19:46"
        final_df.loc[
            final_df["UTC_Time"] == "2022-07-26 07:13:22", "UTC_Time"
        ] = "2022-07-26 07:13:21"
        final_df.loc[
            final_df["UTC_Time"] == "2022-02-21 10:16:31", "UTC_Time"
        ] = "2022-02-21 10:16:32"
        final_df.loc[
            final_df["UTC_Time"] == "2022-01-21 10:13:16", "UTC_Time"
        ] = "2022-01-21 10:13:15"

        final_df.index = [tx.str_to_datetime(j) for j in final_df["UTC_Time"]]

        final_df.loc[
            np.logical_and(
                final_df["UTC_Time"].isin(
                    ["2022-05-31 12:46:17", "2022-08-27 08:51:41"]
                ),
                final_df["Coin"] == "LUNC",
            ),
            "Coin",
        ] = "LUNA"

        for asset in set(final_df["Coin"]):
            final_df.loc[final_df["Coin"] == asset, :] = final_df.loc[
                final_df["Coin"] == asset, :
            ].drop_duplicates(subset=["UTC_Time", "Change"])

        final_df.loc[
            final_df["Operation"] == "Small Assets Exchange BNB", "Operation"
        ] = "Small assets exchange BNB"
        final_df.loc[
            final_df["Operation"] == "Large OTC Trading", "Operation"
        ] = "Large OTC trading"
        final_df.loc[
            final_df["Operation"] == "ETH 2.0 Staking", "Operation"
        ] = "Large OTC trading"
        final_df.loc[
            final_df["Operation"] == "Main and Funding Account Transfer", "Operation"
        ] = "Main and funding account transfer"

        operations_col = [
            "Card Cashback",
            "Rewards Distribution",
            "Simple Earn Locked Rewards",
            "ETH 2.0 Staking Rewards",
            "Launchpad token distribution",
            "Distribution",
            "Cash Voucher distribution",
            "Simple Earn Flexible Interest",
            "Staking Rewards",
            "Super BNB Mining",
            "Launchpool Interest",
            "BNB Vault Rewards",
            "POS savings interest",
            "Savings Interest",
            "Deposit",
            "Withdraw",
            "Fiat Deposit",
            "Binance Card Spending",
            "Auto-Invest Transaction",
            "Buy",
            "Large OTC trading",
            "Fee",
            "Sell",
            "Transaction Related",
            "Small assets exchange BNB",
            "ETH 2.0 Staking",
            "Launchpad subscribe",
            "POS savings purchase",
            "POS savings redemption",
            "Savings Principal redemption",
            "Savings purchase",
            "Simple Earn Flexible Subscription",
            "Simple Earn Flexible Redemption",
            "Simple Earn Locked Subscription",
            "Simple Earn Locked Redemption",
            "Main and funding account transfer",
            "Staking Purchase",
            "Staking Redemption",
        ]

        not_considered = [
            k for k in set(final_df["Operation"]) if k not in operations_col
        ]

        if len(not_considered) > 0:
            print(
                f'WARNING BINANCE: columns {", ".join(not_considered)} are not being included in the calculation'
            )

        for j in final_df["UTC_Time"]:
            tx.str_to_datetime(j)

        final_df.sort_index(inplace=True)

        if raw:
            return final_df

        trades_df = final_df[
            final_df["Operation"].isin(
                [
                    "Buy",
                    "Large OTC Trading",
                    "Large OTC trading",
                    "Fee",
                    "Sell",
                    "Transaction Related",
                    "Small assets exchange BNB",
                ]
            )
        ].copy()

        binance_card = final_df[final_df["Operation"] == "Binance Card Spending"].copy()
        if card_transactions:
            return binance_card

        from_coin, to_coin, from_amount, to_amount, fee, fee_coin, timestamp, tag = [
            [] for i in range(8)
        ]
        for trade_time in set(trades_df.index):
            temp_df = final_df.loc[[trade_time], :]
            if temp_df["Operation"].values[0] == "Small assets exchange BNB":
                group_df = temp_df[["Change", "Coin"]].groupby(by=["Coin"]).sum()
                tag.append("Dust conversion")
                timestamp.append(trade_time)
                fee.append(None)
                fee_coin.append(None)
                to_coin.append("BNB")
                to_val = group_df.loc["BNB", "Change"] / (group_df.shape[0] - 1)
                to_amount.append(to_val)
                group_df = group_df[group_df.index != "BNB"]
                from_amount.append(-abs(group_df.iloc[0, 0]))
                from_coin.append(group_df.index[0])
                group_df = group_df[group_df.index != group_df.index[0]]
                if group_df.shape[0] > 0:
                    for coin in group_df.index:
                        timestamp.append(trade_time)
                        tag.append("Dust conversion")
                        fee.append(None)
                        fee_coin.append(None)
                        to_coin.append("BNB")
                        to_amount.append(to_val)
                        from_amount.append(-abs(group_df.loc[coin, "Change"]))
                        from_coin.append(coin)
            else:
                if final_df.loc[trade_time, :].shape[0] == 2:
                    from_amount.append(
                        -abs(temp_df.loc[temp_df["Change"] < 0, "Change"].values[0])
                    )
                    to_amount.append(
                        temp_df.loc[temp_df["Change"] > 0, "Change"].values[0]
                    )
                    from_coin.append(
                        temp_df.loc[temp_df["Change"] < 0, "Coin"].values[0]
                    )
                    to_coin.append(temp_df.loc[temp_df["Change"] > 0, "Coin"].values[0])
                    fee.append(None)
                    fee_coin.append(None)
                    tag.append("Trade")
                    timestamp.append(trade_time)
                elif final_df.loc[trade_time, :].shape[0] == 3:
                    timestamp.append(trade_time)
                    fee.append(
                        temp_df.loc[temp_df["Operation"] == "Fee", "Change"].values[0]
                    )
                    fee_coin.append(
                        temp_df.loc[temp_df["Operation"] == "Fee", "Coin"].values[0]
                    )
                    temp_df = temp_df[temp_df["Operation"] != "Fee"]
                    from_amount.append(
                        -abs(temp_df.loc[temp_df["Change"] < 0, "Change"].values[0])
                    )
                    to_amount.append(
                        temp_df.loc[temp_df["Change"] > 0, "Change"].values[0]
                    )
                    from_coin.append(
                        temp_df.loc[temp_df["Change"] < 0, "Coin"].values[0]
                    )
                    to_coin.append(temp_df.loc[temp_df["Change"] > 0, "Coin"].values[0])
                    tag.append("Trade")
                elif final_df.loc[trade_time, :].shape[0] > 3:
                    group_trx = (
                        temp_df[["Change", "Operation", "Coin"]]
                        .groupby(by=["Operation", "Coin"])
                        .sum()
                    )
                    operations = [operation for operation, coin in group_trx.index]
                    if "Fee" not in operations:
                        for operation, coin in group_trx.index:
                            fee.append(None)
                            fee_coin.append(None)
                            timestamp.append(trade_time)
                            tag.append("Correct")
                            if group_trx.loc[(operation, coin), "Change"] > 0:
                                to_coin.append(coin)
                                to_amount.append(
                                    group_trx.loc[(operation, coin), "Change"]
                                )
                                from_coin.append(None)
                                from_amount.append(None)
                            else:
                                from_coin.append(coin)
                                from_amount.append(
                                    -abs(group_trx.loc[(operation, coin), "Change"])
                                )
                                to_coin.append(None)
                                to_amount.append(None)
                    else:
                        for operation, coin in group_trx.index:
                            timestamp.append(trade_time)
                            tag.append("Correct")
                            if operation == "Fee":
                                fee.append(group_trx.loc[(operation, coin), "Change"])
                                fee_coin.append(coin)
                                to_coin.append(None)
                                to_amount.append(None)
                                from_coin.append(None)
                                from_amount.append(None)
                            else:
                                fee.append(None)
                                fee_coin.append(None)
                                if group_trx.loc[(operation, coin), "Change"] > 0:
                                    to_coin.append(coin)
                                    to_amount.append(
                                        group_trx.loc[(operation, coin), "Change"]
                                    )
                                    from_coin.append(None)
                                    from_amount.append(None)
                                else:
                                    from_coin.append(coin)
                                    from_amount.append(
                                        -abs(group_trx.loc[(operation, coin), "Change"])
                                    )
                                    to_coin.append(None)
                                    to_amount.append(None)

        auto_df = final_df[final_df["Operation"] == "Auto-Invest Transaction"]
        rec_trx = [k for k in range(auto_df.shape[0]) if auto_df.iloc[k, 5] < 0]
        rec_trx.append(auto_df.shape[0])
        for i, rec in enumerate(rec_trx):
            if rec == auto_df.shape[0]:
                break
            temp_df = auto_df.iloc[rec : rec_trx[i + 1], :]
            in_val = -temp_df.loc[temp_df["Change"] < 0, "Change"].values[0] / (
                temp_df.shape[0] - 1
            )
            in_coin = temp_df.loc[temp_df["Change"] < 0, "Coin"].values[0]
            for k in range(temp_df.loc[temp_df["Change"] > 0].shape[0]):
                tag.append("Recurring")
                fee.append(None)
                fee_coin.append(None)
                to_amount.append(
                    temp_df.loc[temp_df["Change"] > 0, "Change"].tolist()[k]
                )
                to_coin.append(temp_df.loc[temp_df["Change"] > 0, "Coin"].tolist()[k])
                timestamp.append(temp_df.loc[temp_df["Change"] > 0].index[k])
                from_coin.append(in_coin)
                from_amount.append(-abs(in_val))

        trades_df = pd.DataFrame()
        trades_df["From"] = None
        trades_df["To"] = None
        trades_df["From Coin"] = from_coin
        trades_df["To Coin"] = to_coin
        trades_df["From Amount"] = from_amount
        trades_df["To Amount"] = to_amount
        trades_df["Fee"] = [-abs(x) if x is not None else None for x in fee]
        trades_df["Fee Coin"] = fee_coin
        trades_df["Fiat"] = "EUR"
        trades_df["Fiat Price"] = None
        trades_df["Tag"] = tag
        trades_df["Source"] = "Binance"
        trades_df["Notes"] = "Trade"
        trades_df.index = timestamp
        trades_df.sort_index(inplace=True)

        to_correct_df = trades_df[trades_df["Tag"] == "Correct"]
        trades_df = trades_df[trades_df["Tag"] != "Correct"]
        for timest in set(to_correct_df.index):
            temp_df = to_correct_df.loc[[timest], :]
            temp_df.ffill(inplace=True)
            temp_df.bfill(inplace=True)
            if temp_df.shape[0] == 2 or temp_df.shape[0] == 3:
                trades_df = pd.concat([trades_df, temp_df.iloc[[0], :]])
            elif temp_df.shape[0] == 4:
                temp_df.drop_duplicates(inplace=True)
                temp_df.iloc[0, 2:6] = None
                trades_df = pd.concat([trades_df, temp_df])

    movements_df = final_df[
        final_df["Operation"].isin(["Deposit", "Withdraw", "Fiat Deposit"])
    ].copy()
    deposits_df = pd.DataFrame()
    withdraw_df = pd.DataFrame()

    deposits_df["From"] = "External"
    deposits_df["To"] = None
    deposits_df["From Coin"] = None
    deposits_df["To Coin"] = movements_df.loc[
        movements_df["Operation"] != "Withdraw", "Coin"
    ]
    deposits_df["From Amount"] = None
    deposits_df["To Amount"] = movements_df.loc[
        movements_df["Operation"] != "Withdraw", "Change"
    ]
    deposits_df["Fee"] = None
    deposits_df["Fee Coin"] = None
    deposits_df["Fiat"] = "EUR"
    deposits_df["Fiat Price"] = None
    deposits_df["Tag"] = "Deposit"
    deposits_df["Source"] = "Binance"

    withdraw_df["From"] = None
    withdraw_df["To"] = "External"
    withdraw_df["From Coin"] = movements_df.loc[
        movements_df["Operation"] == "Withdraw", "Coin"
    ]
    withdraw_df["To Coin"] = None
    withdraw_df["From Amount"] = -abs(
        movements_df.loc[movements_df["Operation"] == "Withdraw", "Change"]
    )
    withdraw_df["To Amount"] = None
    withdraw_df["Fee"] = None
    withdraw_df["Fee Coin"] = None
    withdraw_df["Fiat"] = "EUR"
    withdraw_df["Fiat Price"] = None
    withdraw_df["Tag"] = "Withdraw"
    withdraw_df["Source"] = "Binance"

    movements_df = pd.concat([deposits_df, withdraw_df])
    movements_df["Notes"] = ""
    del withdraw_df, deposits_df

    # Tutte le operazioni reward
    cashback = final_df.loc[
        final_df["Operation"].isin(
            [
                "Card Cashback",
                "Rewards Distribution",
                "Simple Earn Locked Rewards",
                "ETH 2.0 Staking Rewards",
                "Launchpad token distribution",
                "Distribution",
                "Cash Voucher distribution",
                "Simple Earn Flexible Interest",
                "Staking Rewards",
                "Super BNB Mining",
                "Launchpool Interest",
                "BNB Vault Rewards",
                "POS savings interest",
                "Savings Interest",
            ]
        )
    ].copy()
    cashback["Notes"] = ""

    cashback.loc[cashback["Operation"] == "Card Cashback", "Notes"] = "Cashback"

    cashback["From"] = None
    cashback["To"] = None
    cashback["From Coin"] = None
    cashback["To Coin"] = cashback["Coin"]
    cashback["From Amount"] = None
    cashback["To Amount"] = cashback["Change"]
    cashback["Fee"] = None
    cashback["Fee Coin"] = None
    cashback["Fiat"] = "EUR"
    cashback["Fiat Price"] = None
    cashback["Tag"] = "Reward"
    cashback["Source"] = "Binance"

    cashback.drop(
        ["User_ID", "UTC_Time", "Account", "Operation", "Coin", "Change", "Remark"],
        inplace=True,
        axis=1,
    )

    vout = pd.concat([movements_df, trades_df, cashback])
    vout["Fee Fiat"] = None
    vout.sort_index(inplace=True)

    vout.loc[vout["Notes"] == "Trade", "Tag"] = vout.loc[
        vout["Notes"] == "Trade", "Notes"
    ]
    vout.loc[vout["Notes"] == "Trade", "Notes"] = ""

    global bin_prices
    bin_prices = Prices()

    vout = tx.price_transactions_df(vout, bin_prices)
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
    return vout


def get_binance_card_spending(total_spending=False, year=None):
    vout = get_transactions_df(card_transactions=True)
    if year is not None:
        vout = vout[vout.index.year == year]
    if total_spending:
        vout = abs(vout["Change"].sum())
    return round(vout, 2)


def get_eur_invested(year=None):
    all_trx = get_transactions_df(raw=True)
    if year is not None:
        all_trx = all_trx[all_trx.index.year == year]
        all_trx = all_trx[all_trx["Change"] < 0]
        return -all_trx.loc[
            np.logical_and(
                all_trx["Coin"] == "EUR",
                ~all_trx["Operation"].isin(
                    ["Deposit", "Withdraw", "Fiat Deposit", "Binance Card Spending"]
                ),
            ),
            "Change",
        ].sum()

    all_eur_in = all_trx.loc[
        np.logical_and(
            all_trx["Coin"] == "EUR",
            all_trx["Operation"].isin(["Deposit", "Withdraw", "Fiat Deposit"]),
        ),
        "Change",
    ].sum()
    card_spending = -all_trx.loc[
        all_trx["Operation"] == "Binance Card Spending", "Change"
    ].sum()
    print(f"€{round(all_eur_in, 2)} in with €{round(card_spending, 2)} card spending")
    return all_eur_in - card_spending
