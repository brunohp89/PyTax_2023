import numpy as np
import pandas as pd
import datetime as dt
import requests
from PricesClass import Prices
import matplotlib.pyplot as plt
import os
import sys

fiat_list = [
    "AUD",
    "BRL",
    "EUR",
    "GBP",
    "GHS",
    "HKD",
    "KES",
    "KZT",
    "NGN",
    "NOK",
    "PHP",
    "PEN",
    "RUB",
    "TRY",
    "UGX",
    "UAH",
    "",
]

stable_list = [
    "BUSD",
    "FUSD",
    "USDT",
    "USDC",
    "DAI",
    "LUSD"
]


def str_to_datetime(date: str):
    try:
        if len(date) > 11:
            new_date = dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        else:
            new_date = dt.datetime.strptime(date, "%Y-%m-%d")

        return new_date

    except ValueError:
        print("Invalid format. Allowed formats are: YYYY-MM-DD and YYYY-MM-DD HH:MM:SS")


def datetime_to_str(date, hour_output=True):
    if hour_output:
        s = date.strftime("%Y-%m-%d %H:%M:%S")
    else:
        s = date.strftime("%Y-%m-%d")
    return str(s)


def to_binance_date_format(date_to_convert):
    month_out = None
    if date_to_convert.month == 1:
        month_out = "Jan"
    elif date_to_convert.month == 2:
        month_out = "Feb"
    elif date_to_convert.month == 3:
        month_out = "Mar"
    elif date_to_convert.month == 4:
        month_out = "Apr"
    elif date_to_convert.month == 5:
        month_out = "May"
    elif date_to_convert.month == 6:
        month_out = "Jun"
    elif date_to_convert.month == 7:
        month_out = "Jul"
    elif date_to_convert.month == 8:
        month_out = "Aug"
    elif date_to_convert.month == 9:
        month_out = "Sep"
    elif date_to_convert.month == 10:
        month_out = "Oct"
    elif date_to_convert.month == 11:
        month_out = "Nov"
    elif date_to_convert.month == 12:
        month_out = "Dec"
    return f"{date_to_convert.day} {month_out}, {date_to_convert.year}"


def uphold_date_to_datetime(date):
    month_out = None
    if date[4:7] == "Jan":
        month_out = 1
    elif date[4:7] == "Feb":
        month_out = 2
    elif date[4:7] == "Mar":
        month_out = 3
    elif date[4:7] == "Apr":
        month_out = 4
    elif date[4:7] == "May":
        month_out = 5
    elif date[4:7] == "Jun":
        month_out = 6
    elif date[4:7] == "Jul":
        month_out = 7
    elif date[4:7] == "Aug":
        month_out = 8
    elif date[4:7] == "Sep":
        month_out = 9
    elif date[4:7] == "Oct":
        month_out = 10
    elif date[4:7] == "Nov":
        month_out = 11
    elif date[4:7] == "Dec":
        month_out = 12
    date_out = dt.datetime(
        int(date[11:15]),
        month_out,
        int(date[8:10]),
        int(date[16:18]),
        int(date[19:21]),
        int(date[22:24]),
    )
    return date_out


def turn_age_into_dt(date_input):
    if "days" in date_input:
        days = int(date_input.split(" days ")[0])
        date_input = date_input.split(" days ")[1]
    else:
        days = 0
    if "hrs" in date_input:
        hours = int(date_input.split(" hrs ")[0])
        date_input = date_input.split(" hrs ")[1]
    else:
        hours = 0
    if "minutes" in date_input:
        minutes = int(date_input.split(" minutes ")[0])
        date_input = date_input.split(" minutes ")[1]
    else:
        minutes = 0
    if "secs" in date_input:
        seconds = int(date_input.split(" secs ")[0])
    else:
        seconds = 0

    return dt.datetime.now() - dt.timedelta(
        days=days, hours=hours + 1, minutes=minutes, seconds=seconds
    )


def get_bnb(address):
    # FOR ADDRESSES STARTING WITH BNB (BINANCE CHAIN)
    output_bnb = requests.get(f"https://explorer.bnbchain.world/txs?address={address}")
    output_bnb = pd.read_html(output_bnb.text)
    output_bnb = output_bnb[0]

    output_bnb.index = [turn_age_into_dt(k) for k in output_bnb["Age"]]

    output_bnb["Fee"] = -0.00075
    output_bnb["Value"] = [float(k.replace(" BNB", "")) for k in output_bnb["Value"]]

    output_bnb["Fiat Price"] = 0
    output_bnb["Fiat"] = "EUR"
    output_bnb["Fee Currency"] = "BNB"
    output_bnb["To Coin"] = ""
    output_bnb["Coin"] = "BNB"
    output_bnb["To Amount"] = ""
    output_bnb["Tag Account"] = "BNB Beacon Chain"

    output_bnb.rename(columns={"Value": "Amount", "Type": "Tag"}, inplace=True)

    output_bnb.drop(["TxHash", "Height", "Age", "Unnamed: 5"], axis=1, inplace=True)

    output_bnb.loc[output_bnb["From"] == address, "Amount"] *= -1

    output_bnb["Tag"] = "Movement"

    output_bnb = output_bnb.reindex(
        columns=[
            "From",
            "To",
            "Coin",
            "Amount",
            "To Coin",
            "To Amount",
            "Fiat Price",
            "Fiat",
            "Fee",
            "Fee Currency",
            "Tag",
        ]
    )
    output_bnb.sort_index(inplace=True)

    return output_bnb


def get_fiat_investment(
        transactions_df,
        currency="EUR",
        cummulative=True,
        year_sel="all",
        **credit_card_transactions,
):
    if credit_card_transactions is not None:
        ind_extra = []
        data_extra = []
        for cct in credit_card_transactions:
            cct1 = cct.replace("dt_", "")
            cct1 = cct1.replace("_", "-")
            ind_extra.append(str_to_datetime(cct1))
            data_extra.append(-credit_card_transactions[cct])
        FiatExtra = pd.DataFrame(
            pd.Series(data=data_extra, index=ind_extra, dtype=float)
        )
        FiatExtra.index = [p.date() for p in FiatExtra.index]
        FiatExtra.columns = ["Amount"]

    in_fiat = transactions_df[
        np.logical_and(
            np.logical_or(
                transactions_df["Coin"] == currency,
                transactions_df["To Coin"] == currency,
            ),
            transactions_df["To Coin"] != "",
        )
    ]
    NewAmount = in_fiat.loc[in_fiat["Coin"] != currency, "To Amount"].tolist()
    in_fiat.loc[in_fiat["Coin"] != currency, "Amount"] = NewAmount

    if (
            in_fiat[in_fiat["Amount"] > 0].shape[0] > 0
            and in_fiat[in_fiat["Amount"] < 0].shape[0] > 0
    ):
        in_fiat = in_fiat[in_fiat["Amount"] > 0]
        in_fiat["Amount"] *= -1
    if in_fiat.shape[0] > 0:
        in_fiat.index = [p.date() for p in in_fiat.index]
        if credit_card_transactions is not None:
            in_fiat = pd.concat([in_fiat, FiatExtra], axis=0)
        in_fiat = in_fiat.groupby(in_fiat.index).sum()
        in_fiat = pd.DataFrame(-in_fiat["Amount"])

        if year_sel != "all":
            in_fiat = in_fiat[in_fiat.index >= dt.date(year_sel, 1, 1)]
            in_fiat = in_fiat[in_fiat.index <= dt.date(year_sel, 12, 31)]
        if cummulative:
            in_fiat = in_fiat.cumsum()
    else:
        if credit_card_transactions is not None:
            in_fiat = -FiatExtra.copy()
            if year_sel != "all":
                in_fiat = in_fiat[in_fiat.index >= dt.date(year_sel, 1, 1)]
                in_fiat = in_fiat[in_fiat.index <= dt.date(year_sel, 12, 31)]
            if cummulative:
                in_fiat = in_fiat.cumsum()
    if in_fiat.shape[0] == 0:
        if year_sel == "all":
            year_sel = 2021
        ind_out = pd.date_range(
            dt.date(year_sel, 1, 1),
            dt.datetime.today().date() - dt.timedelta(days=1),
            freq="d",
        )
        data = [0] * len(ind_out)
        in_fiat = pd.DataFrame(pd.Series(data=data, index=ind_out))
    in_fiat.columns = [currency]
    in_fiat.sort_index(inplace=True)

    return in_fiat


def write_excel(file_name, **sheets):
    excel_writer = pd.ExcelWriter(file_name, engine="xlsxwriter")
    for sheet in sheets:
        sheets[sheet].to_excel(excel_writer, sheet_name=sheet)

    excel_writer.close()
    print(f"Excel file output - {file_name}")


def calcolo_giacenza_media(df):
    return (
            df.sum(axis=1)[df.sum(axis=1) != 0].sum(axis=0)
            / df.sum(axis=1)[df.sum(axis=1) != 0].shape[0]
    )


def join_dfs(**df_to_join):
    df_in = pd.DataFrame()
    for df in df_to_join:
        if df_to_join[df].shape[0] == 0:
            continue
        df_to_join[df].columns = [p.upper() for p in df_to_join[df].columns]
        if df_in.shape[0] == 0:
            df_in = df_to_join[df].copy()
        else:
            df_in = df_in.join(df_to_join[df], rsuffix="--R").copy()
        df_in.iloc[0, :].fillna(0, inplace=True)
        df_in.ffill(inplace=True)

    df_in.columns = [l.replace("--R", "") for l in df_in.columns]
    df_in = df_in.groupby(by=df_in.columns, axis=1).sum()
    return df_in


def concat_dfs(**df_to_concat):
    df_in = pd.DataFrame()
    for df in df_to_concat:
        if df_to_concat[df].shape[0] == 0:
            continue
        if df_in.shape[0] == 0:
            df_in = df_to_concat[df].copy()
        else:
            if df_in.shape[1] != df_to_concat[df].shape[1]:
                print(f"{df} --> PROBLEMI DI DIMENSIONE")
            df_in = pd.concat([df_in, df_to_concat[df]], axis=0)
    df_in.sort_index(inplace=True)
    return df_in


def get_primo_ultimo_giorno(df, tax_year):
    if dt.datetime.today().date() <= dt.date(tax_year, 12, 31):
        ultimo_giorno = "Non disponibile"
    else:
        ultimo_giorno = round(df.sum(axis=1)[-1], 2)
    primo_giorno = round(df.sum(axis=1)[0], 2)
    return [primo_giorno, ultimo_giorno]


def balances_fiat(
        balances: pd.DataFrame, prices=Prices(), currency="eur", year_sel=None
):
    balances_in = balances.copy()
    balances_in.columns = [x.upper() for x in balances_in.columns]

    for coin in list(balances_in.columns):
        if coin not in prices.prices["Prices"][currency.upper()].keys():
            balances_in[coin] *= 0
        else:
            temp_df = pd.merge(
                balances_in[coin],
                prices.prices["Prices"][currency.upper()][coin.upper()],
                left_index=True,
                right_index=True,
            )
            temp_df[coin.upper()] *= temp_df.iloc[:, 1:5].mean(axis=1)
            balances_in[coin] = temp_df[coin.upper()]
    balances_in.fillna(0, inplace=True)

    if year_sel is not None:
        temp_df = balances_in[balances_in.index >= dt.date(year_sel, 1, 1)].copy()
        temp_df = temp_df[temp_df.index <= dt.date(year_sel, 12, 31)].copy()
        return temp_df
    else:
        return balances_in


def prepare_df(
        df_in: pd.DataFrame, year_sel=None, cummulative=True, allow_negative=False
):
    # Il df dev'essere un dataframe con index orario senza NaN
    df = df_in.copy()

    if isinstance(df_in.index[0], dt.datetime):
        df.index = [k.date() for k in df.index]
        temp_df = df.groupby(df.index).sum()
    else:
        temp_df = df_in.copy()

    fill_na_index = pd.date_range(
        dt.date(min(df.index).year, 1, 1), dt.date.today() - dt.timedelta(days=1)
    )
    fill_na_index = [
        str_to_datetime(a.date().isoformat()).date() for a in fill_na_index
    ]
    fill_na = pd.DataFrame(index=fill_na_index, data=np.zeros([len(fill_na_index), 1]))
    temp_df = temp_df.join(fill_na, how="outer")
    temp_df.drop([0], axis=1, inplace=True)
    temp_df.fillna(0, inplace=True)
    if cummulative:
        temp_df = temp_df.cumsum()
    if year_sel is not None:
        temp_df = temp_df[temp_df.index >= dt.date(year_sel, 1, 1)]
        temp_df = temp_df[temp_df.index <= dt.date(year_sel, 12, 31)]

    temp_df = temp_df.loc[
              :,
              ~temp_df.columns.isin(
                  list(temp_df.sum(axis=0)[temp_df.sum(axis=0) == 0].index)
              ),
              ]

    if not allow_negative:
        temp_df[temp_df < 10 ** -9] = 0
    return temp_df


def balances(transactions: pd.DataFrame, cummulative=True, year_sel=None, allow_negative=False):
    # Obtain daily balances in native cryptocurrency
    from_df = transactions[["From Coin", "From Amount"]].copy()
    to_df = transactions[["To Coin", "To Amount"]].copy()
    fees = transactions[["Fee Coin", "Fee"]].copy()
    fees.columns = ["Coin", "Amount"]
    from_df.columns = ["Coin", "Amount"]
    to_df.columns = ["Coin", "Amount"]

    balance_df = pd.concat([from_df, to_df, fees])
    balance_df = balance_df[balance_df["Coin"] != "EUR"]

    balance_df = balance_df[~pd.isna(balance_df["Coin"])]
    balance_df['date'] = [k.date() for k in balance_df.index]

    currencies = list(np.unique(balance_df["Coin"]))
    if currencies[0] == '':
        currencies.pop(0)

    temp_df = pd.DataFrame()
    for index, currency in enumerate(currencies):
        if index == 0:
            temp_df = pd.DataFrame(
                balance_df.loc[balance_df["Coin"] == currency, ["Amount", 'date']]
            )
            temp_df.sort_index(inplace=True)
            temp_df = temp_df.groupby('date').sum()
            temp_df.columns = [currency]
        else:
            colnames = list(temp_df.columns)
            colnames.append(currency)
            new_df = pd.DataFrame(balance_df.loc[balance_df["Coin"] == currency, ["Amount", 'date']]).groupby(
                'date').sum()
            temp_df = temp_df.join(
                new_df,
                how="outer",
            )
            temp_df.sort_index(inplace=True)
            temp_df.columns = colnames

    temp_df.fillna(0, inplace=True)
    return prepare_df(temp_df, year_sel, cummulative, allow_negative)


def price_transactions_df(df_in: pd.DataFrame, prices_in: Prices, only_fee=False):
    df_in = df_in.sort_index()
    nfts = []
    tokens = df_in["Fee Coin"].tolist()
    if not only_fee:
        tokens.extend(df_in["To Coin"].tolist())
        tokens.extend(df_in["From Coin"].tolist())
    tokens = [
        x.upper() for x in list(set(tokens)) if
        x not in fiat_list and not pd.isna(x) and x not in nfts and x is not None
    ]

    df_in["To Coin"] = [c.upper() if ~pd.isna(c) and c is not None and not isinstance(c,float) else None for c in df_in["To Coin"]]
    df_in["From Coin"] = [c.upper() if ~pd.isna(c) and c is not None and not isinstance(c,float) else None for c in df_in["From Coin"]]

    prices_in.get_prices(tokens)
    prices_in.convert_prices(tokens, "EUR")

    if not only_fee:
        df_in.loc[df_in["From Coin"] == "EUR", "Fiat Price"] = df_in.loc[
            df_in["From Coin"] == "EUR", "From Amount"
        ]
        df_in.loc[df_in["To Coin"] == "EUR", "Fiat Price"] = df_in.loc[
            df_in["To Coin"] == "EUR", "To Amount"
        ]

    for tok in tokens:
        if not only_fee:
            temp_df = df_in[
                np.logical_and(df_in["From Coin"] == tok, pd.isna(df_in["Fiat Price"]))
            ]
            if temp_df.shape[0] > 0:
                temp_df.index = [k.date() for k in temp_df.index]
                fiat_prices = pd.merge(
                    prices_in.prices["Prices"]["EUR"][tok.upper()][
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
                df_in.loc[
                    np.logical_and(
                        df_in["From Coin"] == tok, pd.isna(df_in["Fiat Price"])
                    ),
                    "Fiat Price",
                ] = fiat_prices

            temp_df = df_in[
                np.logical_and(df_in["To Coin"] == tok, pd.isna(df_in["Fiat Price"]))
            ]
            if temp_df.shape[0] > 0:
                temp_df.index = [k.date() for k in temp_df.index]
                fiat_prices = pd.merge(
                    prices_in.prices["Prices"]["EUR"][tok.upper()][
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
                df_in.loc[
                    np.logical_and(
                        df_in["To Coin"] == tok, pd.isna(df_in["Fiat Price"])
                    ),
                    "Fiat Price",
                ] = fiat_prices

        temp_df = df_in[
            np.logical_and(df_in["Fee Coin"] == tok, pd.isna(df_in["Fee Fiat"]))
        ]
        if temp_df.shape[0] > 0:
            temp_df.index = [k.date() for k in temp_df.index]
            fiat_prices = pd.merge(
                prices_in.prices["Prices"]["EUR"][tok.upper()][
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
            df_in.loc[
                np.logical_and(df_in["Fee Coin"] == tok, pd.isna(df_in["Fee Fiat"])),
                "Fee Fiat",
            ] = fiat_prices
    return df_in


def plot_balances(
        df, columns=None, aggregate=False, colors=None, start_date=None, end_date=None
):
    """
    Plots a pandas dataframe with a date index.
    :param df: The dataframe to plot
    :param columns: The columns to plot (default is all columns)
    :param aggregate: The aggregation method to use (default is no aggregation)
    :param colors: The colors to use for the plot (default is None)
    :param start_date: The start date of the date range to filter by (default is None)
    :param end_date: The end date of the date range to filter by (default is None)
    """
    if start_date is not None and end_date is not None:
        if start_date > end_date:
            raise ValueError("End date has to be after start date")
    # Use all columns if none are specified
    if start_date:
        df = df[df.index >= start_date]
    if end_date:
        df = df[df.index <= end_date]

    if columns is None:
        columns = df.columns
    # Aggregate data if specified
    if aggregate:
        df = df[columns].sum(axis=1)
        if colors:
            ax = df.plot(color=colors)

        else:
            ax = df.plot()

    # Plot the data
    else:
        if colors:
            ax = df[columns].plot(color=colors)
        else:
            ax = df[columns].plot()
    ax.set_xlabel("Date")
    ax.set_ylabel("Value")
    plt.xticks(rotation=45)
    plt.show()


def income(
        transactions: pd.DataFrame,
        type_out="fiat",
        cummulative=True,
        year_sel=None,
        name=None,
        include_cashback=True,
        allow_negative=True,
):
    # Obtain daily income (earn products/supercharge) in cryptocurrency of native fiat
    rendita = transactions[transactions["Tag"].isin(["Reward", "Interest"])].copy()
    if not include_cashback:
        rendita = rendita[rendita["Notes"] != "Cashback"]
    if rendita.shape[0] == 0:
        if type_out == "fiat":
            print(f"No income for {name}")
        return pd.DataFrame()

    rendita['From Amount'] = rendita['From Amount'].fillna(0)
    rendita['Fiat Price'] = [-x if y < 0 else x for x,y in zip(rendita['Fiat Price'], rendita['From Amount'])]

    temp_df_fiat = pd.DataFrame()
    temp_df_token = pd.DataFrame()
    tokens = rendita["To Coin"].tolist()
    tokens.extend(rendita["From Coin"].tolist())
    tokens = [k for k in tokens if k != ""]
    tokens = [k for k in tokens if ~pd.isna(k)]
    tokens = [k for k in tokens if k is not None]

    for col in ["From Coin", "To Coin"]:
        for index, tok in enumerate(np.unique(tokens)):
            temp_df = rendita[rendita[col] == tok]
            if temp_df.shape[0] == 0:
                continue
            if index == 0 and col != 'To Coin':
                temp_df_token = pd.DataFrame(temp_df[f'{col.split(" ")[0]} Amount'])
                temp_df_fiat = pd.DataFrame(temp_df["Fiat Price"])
                temp_df_fiat.columns = temp_df_token.columns = [tok]
            else:
                colnames = list(temp_df_fiat.columns)
                colnames.append(tok)
                temp_df_token = temp_df_token.join(
                    pd.DataFrame(temp_df[f'{col.split(" ")[0]} Amount']), how="outer"
                )
                temp_df_fiat = temp_df_fiat.join(
                    pd.DataFrame(temp_df["Fiat Price"]), how="outer"
                )
                temp_df_fiat.columns = temp_df_token.columns = colnames

    temp_df_fiat.fillna(0, inplace=True)
    temp_df_token.fillna(0, inplace=True)

    temp_df_token = temp_df_token.groupby(lambda x: x, axis=1).sum()
    temp_df_fiat = temp_df_fiat.groupby(lambda x: x, axis=1).sum()

    if year_sel is not None:
        temp_df_fiat[temp_df_fiat.index < dt.datetime(year_sel, 1, 1, 0, 0, 0)] = 0
        temp_df_token[temp_df_token.index < dt.datetime(year_sel, 1, 1, 0, 0, 0)] = 0
    if type_out == "fiat":
        return prepare_df(temp_df_fiat, year_sel, cummulative, allow_negative)
    else:
        return prepare_df(temp_df_token, year_sel, cummulative, allow_negative)


def import_script(script_name):
    try:
        with open(os.path.join(os.path.abspath("calculators"), script_name), 'r') as f:
            code = compile(f.read(), script_name, 'exec')
            exec(code, globals())
        print(f"Script '{script_name}' imported successfully")
    except FileNotFoundError:
        print(f"Script '{script_name}' not found")


def import_function_from_script(module_name, function_name):
    sys.path.append(os.path.abspath("calculators"))
    module = __import__(module_name)
    imported_function = getattr(module, function_name)
    globals()[function_name] = imported_function
    return imported_function


def generate_xlsx(file_name, sheet_names, data):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    for sheet_name, sheet_data in zip(sheet_names, data):
        # Write each dataframe to a different worksheet.
        sheet_data.to_excel(writer, sheet_name=sheet_name)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
