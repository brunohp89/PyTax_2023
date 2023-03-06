from PricesClass import Prices
import tax_library as tx
import datetime as dt
import pandas as pd
import requests
import os
import json

scamtokens = ["0x5c7f8a570d578ed84e63fdfa7b1ee72deae1ae23"]


# The transactions on Crypto.org chain have to be extracted manually, refer to the example file
def get_crypto_dot_org_transactions():
    cronos_files = [
        os.path.join(os.path.abspath('cryptodotorg'), x)
        for x in os.listdir(os.path.abspath('cryptodotorg'))
        if "automatico" not in x
    ]
    if len(cronos_files) == 0:
        print("No files for crypto.org found")
        return None
    else:
        df_list = []
        for filename in cronos_files:
            df_loop = pd.read_csv(filename, index_col=None, header=0)
            df_list.append(df_loop)
        final_df = pd.concat(df_list, axis=0, ignore_index=True)
        final_df.index = [
            tx.str_to_datetime(x.replace(" UTC", "")) + dt.timedelta(hours=1)
            for x in list(final_df["Timestamp"])
        ]

        final_df.drop(["Timestamp"], axis=1, inplace=True)

        final_df["Fiat Price"] = None
        final_df["Fiat"] = "EUR"
        final_df["Fee Coin"] = "CRO"
        final_df["To Coin"] = None
        final_df["To Amount"] = None
        final_df["Tag"] = "Movement"
        final_df["Fee Fiat"] = None
        final_df["Notes"] = None
        final_df["Source"] = "Cronos Chain"

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

    return final_df


def get_transactions_df(address, beacon_address=None):
    address = address.lower()
    beacon = pd.DataFrame()
    if beacon_address is not None:
        beacon = get_crypto_dot_org_transactions()
        beacon["Fee"] *= -1
        beacon["To"] = beacon["To"].map(lambda x: x.lower())
        beacon["From"] = beacon["From"].map(lambda x: x.lower())
        beacon.loc[beacon["From"] == beacon_address, "From Amount"] *= -1
        beacon.sort_index(inplace=True)
        beacon.loc[
            beacon["To"] == address, "From Amount"
        ] *= 0  # Transfers between same wallets set to zero

    with open(os.getcwd() + "\\.json") as creds:
        apikey = json.load(creds)["CronosScanToken"]

    if apikey == "":
        raise PermissionError("No API KEY for Cronos Scan found in .json")

    # NORMAL TRANSACTIONS
    url = f"https://api.cronoscan.com/api?module=account&action=txlist&address={address}&startblock=1&endblock=999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)
    normal_transactions = pd.DataFrame(response.json().get("result"))
    normal_transactions = normal_transactions[
        normal_transactions["isError"] != 1
    ].copy()
    normal_transactions.reset_index(inplace=True, drop=True)
    normal_transactions["Coin"] = "CRO"

    normal_transactions["from"] = normal_transactions["from"].map(lambda x: x.lower())
    normal_transactions["to"] = normal_transactions["to"].map(lambda x: x.lower())
    normal_transactions["value"] = [
        -int(normal_transactions.loc[i, "value"]) / 10**18
        if normal_transactions.loc[i, "from"] == address.lower()
        else int(normal_transactions.loc[i, "value"]) / 10**18
        for i in range(normal_transactions.shape[0])
    ]
    normal_transactions["gas"] = [
        -(
            int(normal_transactions.loc[i, "gasUsed"])
            * int(normal_transactions.loc[i, "gasPrice"])
        )
        / 10**18
        for i in range(normal_transactions.shape[0])
    ]

    normal_transactions["functionName"] = normal_transactions["functionName"].map(
        lambda x: x.split("(")[0]
    )

    normal_transactions.drop(
        [
            "blockNumber",
            "blockHash",
            "nonce",
            "transactionIndex",
            "gasPrice",
            "input",
            "gasUsed",
            "methodId",
            "contractAddress",
            "isError",
            "cumulativeGasUsed",
            "confirmations",
            "txreceipt_status",
        ],
        axis=1,
        inplace=True,
    )

    # INTERNAL TRANSACTIONS
    url = f"https://api.cronoscan.com/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={apikey}"
    response_internal = requests.get(url)
    internal_transactions = pd.DataFrame(response_internal.json().get("result"))
    internal_transactions = internal_transactions[
        internal_transactions["isError"] != 1
    ].copy()
    internal_transactions.reset_index(inplace=True, drop=True)

    internal_transactions["from"] = internal_transactions["from"].map(
        lambda x: x.lower()
    )
    internal_transactions["to"] = internal_transactions["to"].map(lambda x: x.lower())
    internal_transactions["value"] = [
        -int(internal_transactions.loc[i, "value"]) / 10**18
        if internal_transactions.loc[i, "from"] == address.lower()
        else int(internal_transactions.loc[i, "value"]) / 10**18
        for i in range(internal_transactions.shape[0])
    ]

    internal_transactions.drop(
        [
            "blockNumber",
            "contractAddress",
            "input",
            "type",
            "gas",
            "gasUsed",
        ],
        axis=1,
        inplace=True,
    )

    # CRC20 TRANSACTIONS
    url = f"https://api.cronoscan.com/api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={apikey}"
    response = requests.get(url)
    erc20_transactions = pd.DataFrame(response.json().get("result"))

    erc20_transactions["from"] = erc20_transactions["from"].map(lambda x: x.lower())
    erc20_transactions["to"] = erc20_transactions["to"].map(lambda x: x.lower())

    erc20_transactions.reset_index(inplace=True, drop=True)

    erc20_transactions["value"] = [
        int(s) / 10 ** int(x)
        for s, x in zip(erc20_transactions["value"], erc20_transactions["tokenDecimal"])
    ]
    erc20_transactions["gas"] = [
        -(
            int(erc20_transactions.loc[i, "gasUsed"])
            * int(erc20_transactions.loc[i, "gasPrice"])
        )
        / 10**18
        for i in range(erc20_transactions.shape[0])
    ]

    erc20_transactions = erc20_transactions[
        ~erc20_transactions["from"].isin(scamtokens)
    ]

    erc20_transactions.drop(
        [
            "blockNumber",
            "tokenDecimal",
            "nonce",
            "blockHash",
            "transactionIndex",
            "gasPrice",
            "contractAddress",
            "cumulativeGasUsed",
            "gasUsed",
            "confirmations",
            "input",
            "tokenName",
        ],
        axis=1,
        inplace=True,
    )

    erc20_transactions.loc[erc20_transactions["from"] == address.lower(), "value"] *= -1

    # -------------------------------------------------------------------------------------------------------------------

    all_trx = pd.merge(
        normal_transactions,
        internal_transactions,
        how="outer",
        on="hash",
        suffixes=("-N", "-I"),
    )
    all_trx = pd.merge(
        all_trx, erc20_transactions, how="outer", on="hash", suffixes=("", "-C")
    )

    all_trx.loc[all_trx["value-N"] == 0, "value-N"] = None

    all_trx["value-N"] = all_trx["value-N"].combine_first(
        all_trx["value-I"]
    )  # .combine_first(all_trx['value'])

    all_trx.index = [
        dt.datetime.fromtimestamp(int(x))
        for x in list(
            all_trx["timeStamp-N"]
            .combine_first(all_trx["timeStamp-I"])
            .combine_first(all_trx["timeStamp"])
        )
    ]

    all_trx = all_trx[
        [
            "hash",
            "from-N",
            "to-N",
            "value-N",
            "Coin",
            "value",
            "tokenSymbol",
            "gas",
            "functionName",
        ]
    ].copy()
    all_trx["Tag"] = ""
    all_trx["Notes"] = ""

    all_trx.sort_index(inplace=True)

    all_trx = all_trx[~all_trx["tokenSymbol"].str.contains("\-LP", na=False)].copy()

    all_trx.loc[
        all_trx["functionName"].str.contains("withdraw"), "Tag"
    ] = "Reward"  # Removing from Farms

    liquidity_df = all_trx[all_trx["functionName"].str.contains("Liquidity")]
    all_trx = all_trx[~all_trx["hash"].isin(list(liquidity_df["hash"]))]
    for token in set(liquidity_df["tokenSymbol"]):
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token]
        if len(temp_df[temp_df["functionName"].str.contains("add")]) > len(
            temp_df[temp_df["functionName"].str.contains("remove")]
        ):
            temp_df = temp_df.iloc[0:-1, :]
        coin1 = temp_df["value-N"].sum()
        coin2 = temp_df["value"].sum()
        index_remove = temp_df.index[-1]
        temp_df = temp_df.iloc[0:2, :]
        temp_df["Coin"] = ["CRO", token]
        temp_df["tokenSymbol"] = None
        temp_df["value"] = None
        temp_df["value-N"] = [coin1, coin2]
        temp_df["Tag"] = "Reward"
        temp_df["Notes"] = "Liquidity Pool"
        temp_df.index = [index_remove] * 2
        all_trx = pd.concat([all_trx, temp_df])

    all_trx.loc[all_trx["functionName"].str.contains("swap"), "Tag"] = "Trade"
    all_trx.loc[all_trx["Tag"] == "", "Tag"] = "Movement"
    all_trx.drop(["functionName", "hash"], axis=1, inplace=True)

    all_trx.columns = [
        "From",
        "To",
        "From Amount",
        "From Coin",
        "To Amount",
        "To Coin",
        "Fee",
        "Tag",
        "Notes",
    ]

    all_trx["Fiat Price"] = None
    all_trx["Fiat"] = "EUR"
    all_trx["Fee Coin"] = "CRO"
    all_trx["Fee Fiat"] = None
    all_trx["Source"] = f"CRO-{address[0:5]}"

    if beacon_address is not None:
        vout = pd.concat([all_trx, beacon])
    else:
        vout = all_trx.copy()

    vout.sort_index(inplace=True)

    sub1 = vout.loc[vout["From Amount"] > 0, ["From Amount", "From Coin"]]
    sub2 = vout.loc[vout["From Amount"] > 0, ["To Amount", "To Coin"]]
    vout.loc[vout["From Amount"] > 0, ["To Amount", "To Coin"]] = sub1.values
    vout.loc[vout["From Amount"] > 0, ["From Amount", "From Coin"]] = sub2.values

    vout["From Coin"] = vout["From Coin"].apply(
        lambda x: None if pd.isna(x) else x.upper()
    )
    vout["To Coin"] = vout["To Coin"].apply(lambda x: None if pd.isna(x) else x.upper())

    vout = tx.price_transactions_df(vout, Prices())
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
