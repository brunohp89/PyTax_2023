import PyPDF2
import datetime as dt
import re
import pandas as pd
import tax_library as tx
from PricesClass import Prices


def get_transactions_df(cashback_level=0.03, returnOnlyAvailablePlu=False):
    reader = PyPDF2.PdfReader('plutus/transactions.pdf')
    amounts = []
    dates = []
    notes = []
    perk = []
    exp = '(\d{4}/\d{2}/\d{2}, \d{2}:\d{2})'
    for page in range(len(reader.pages)):
        page_text = reader.pages[page].extract_text()
        transactions_list = page_text.split('\n')
        for transaction in transactions_list:
            if len(re.findall(exp, transaction)) > 0:
                if 'Account Statement' not in transaction and 'Deposit' not in transaction:
                    dates.append(dt.datetime.strptime(re.findall(exp, transaction)[0].replace(',', ''),
                                                      '%Y/%d/%m %H:%M'))  # + dt.timedelta(days=45))
                    if float(transaction.split('€')[1]) < 0:
                        amounts.append(abs(float(transaction.split('€')[1].replace('\n', ''))))
                    else:
                        amounts.append(0)
                    notes.append(transaction)
                    perk.append(None)
                if 'SPOTIFY' in transaction or 'NETFLIX' in transaction:
                    perk[-1] = 'Perk'

    final_df = pd.DataFrame()

    final_df.index = dates
    final_df['To Amount'] = 1
    final_df['To Coin'] = 'PLU'
    final_df['Notes'] = 'Cashback'
    final_df['Tag'] = 'Reward'
    final_df['Source'] = 'Plutus Card Cashback'
    final_df['Notes 2'] = notes
    final_df['Perk'] = perk
    final_df[[
        "From",
        "To",
        "From Coin",
        "From Amount",
        "Fee",
        "Fee Coin",
        "Fee Fiat",
        "Fiat",
        "Fiat Price",
    ]] = None

    final_df.loc[[dt.datetime(2023, 10, 25, 11, 4)], 'Perk'] = 'Perk'
    final_df['To Amount Cash'] = [float(x) * cashback_level if y != 'Perk' else (float(x) - 10) * cashback_level + 10
                                  for x, y in zip(amounts, final_df['Perk'])]

    final_df = tx.price_transactions_df(final_df, Prices())
    final_df['To Amount'] = final_df['To Amount Cash'] / final_df['Fiat Price']

    final_df = final_df.drop(['To Amount Cash', 'Perk'], axis=1)
    final_df['Fiat Price'] *= final_df['To Amount']

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

    if returnOnlyAvailablePlu:
        final_df['unlock'] = [x + dt.timedelta(days=45) for x in final_df.index]
        final_df = final_df[final_df['unlock'] < dt.datetime.now()]
        final_df = final_df.drop('unlock', axis=1)

    return final_df