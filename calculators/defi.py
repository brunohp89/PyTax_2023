import pandas as pd
import calculators.evm_utils as eu
import numpy as np


def uniswap(df, address, columns_out, gas_coin):
    uniswap_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    if df.shape[0] > 0:
        # Function multicall V2
        multicall = df[
            np.logical_and(
                df["functionName"].str.contains("multicall"),
                df["to_normal"].isin(["0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
                                      "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"]))].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["value"] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)
        multicall["value_normal"] = eu.calculate_value_eth(multicall.value_normal)
        multicall["value_internal"] = eu.calculate_value_eth(multicall.value_internal)

        multicall.loc[multicall['from_internal'] == address, 'value_internal'] *= -1
        multicall.loc[multicall['from_normal'] == address, 'value_normal'] *= -1

        multicall["value_normal"] += multicall["value_internal"]
        multicall["value_normal"] = multicall["value_normal"].abs()

        multicall["from_normal"] = multicall["from_normal"].combine_first(
            multicall["from_internal"].fillna('').apply(lambda x: x.lower()))
        multicall["to_normal"] = multicall["to_normal"].combine_first(
            multicall["to_internal"].fillna('').apply(lambda x: x.lower()))

        multicall["from"] = multicall["from"].fillna('').apply(lambda x: x.lower())
        multicall["to"] = multicall["to"].fillna('').apply(lambda x: x.lower())

        count_df = multicall.groupby(multicall.index).agg({'hash': 'count'}).reset_index()
        if any(count_df['hash'] == 2):
            count_df.index = count_df['timeStamp_normal']
            count_df = count_df.drop('timeStamp_normal', axis=1)
            multicall = pd.merge(count_df, multicall, left_on='timeStamp_normal', right_index=True)
            for indexl in multicall[multicall['hash_x'] == 2].index.unique():
                if multicall[multicall.index == indexl]['value_internal'].iloc[0] != \
                        multicall[multicall.index == indexl]['value_internal'].iloc[1]:
                    multicall.loc[multicall.index == indexl, 'value_internal'] = multicall.loc[
                        multicall.index == indexl, 'value_internal'].sum()

                if multicall[multicall.index == indexl]['value'].iloc[0] != \
                        multicall[multicall.index == indexl]['value'].iloc[1]:
                    multicall.loc[multicall.index == indexl, 'value'] = multicall.loc[
                        multicall.index == indexl, 'value'].sum()

        multicall.loc[multicall["to"] == address, "From Coin"] = gas_coin
        multicall.loc[multicall["to"] == address, "To Coin"] = multicall.loc[multicall["to"] == address, "tokenSymbol"]

        multicall.loc[multicall["from"] == address, "To Coin"] = gas_coin
        multicall.loc[multicall["from"] == address, "From Coin"] = multicall.loc[
            multicall["from"] == address, "tokenSymbol"]

        multicall.loc[multicall["to"] == address, "From Amount"] = -multicall.loc[
            multicall["to"] == address, "value_normal"]
        multicall.loc[multicall["to"] == address, "To Amount"] = multicall.loc[multicall["to"] == address, "value"]

        multicall.loc[multicall["from"] == address, "To Amount"] = multicall.loc[
            multicall["from"] == address, "value_normal"]
        multicall.loc[multicall["from"] == address, "From Amount"] = -multicall.loc[
            multicall["from"] == address, "value"]

        multicall["Fee"] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

        multicall["Tag"] = "Trade"
        multicall["Notes"] = "Uniswap - multicall"

        if any(count_df['hash'] == 2):
            multicall = multicall.drop_duplicates(subset=columns_out)
            multicall = multicall.drop('hash_x', axis=1)

        uniswap_out = pd.concat([uniswap_out, multicall])

        # Multicall V3
        multicall = df[
            np.logical_and(
                df["functionName"].str.contains("multicall"),
                df["to_normal"] == "0xc36442b4a4522e871399cd717abdd847ab11fe88",
            )
        ].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["Fee"] = eu.calculate_gas(
            multicall.gasPrice, multicall.gasUsed_normal
        )
        multicall.loc[pd.isna(multicall["tokenSymbol_erc721"]), ["Tag", "Notes"]] = [
            "Movement",
            "Uniswap-V3 Deposit",
        ]
        multicall.loc[~pd.isna(multicall["tokenSymbol_erc721"]), ["Tag", "Notes"]] = [
            "Movement",
            "Uniswap-V3 Withdraw",
        ]

        uniswap_out = pd.concat([uniswap_out, multicall])

        # Adding and removing liquidity with ETH
        liquidity_df = df[
            np.logical_or(
                df["functionName"].apply(lambda x: x.lower()).str.contains("addliquidityeth"),
                df["functionName"].apply(lambda x: x.lower()).str.contains("removeliquidity"),
            )
        ].copy()

        df = pd.concat([df, liquidity_df]).drop_duplicates(keep=False)

        liquidity_df["functionName"] = liquidity_df["functionName"].apply(lambda x: x.lower())
        liquidity_df = liquidity_df[liquidity_df["tokenSymbol"] != "UNI-V2"]

        liquidity_df["Fee"] = eu.calculate_gas(
            liquidity_df.gasPrice, liquidity_df.gasUsed_normal
        )

        liquidity_df["value"] = eu.calculate_value_token(
            liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
        )
        liquidity_df["value_normal"] = eu.calculate_value_eth(
            liquidity_df.value_normal.fillna(0)
        )
        liquidity_df["value_internal"] = eu.calculate_value_eth(
            liquidity_df.value_internal.fillna(0)
        )
        liquidity_df["value_normal"] += liquidity_df["value_internal"]

        liquidity_df.loc[
            pd.isna(liquidity_df["to_internal"]), "to_internal"
        ] = liquidity_df.loc[pd.isna(liquidity_df["to_internal"]), "to_normal"]

        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("addliquidityeth"),
            ["value_normal", "value"],
        ] *= -1
        liquidity_df = liquidity_df.sort_index()

        for token in liquidity_df["tokenSymbol"].unique():
            temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token]
            if token == 'LOVE':
                temp_df["value"] = temp_df["value"].cumsum()
                temp_df["value_normal"] = temp_df["value_normal"].cumsum()
                temp_df.loc[
                    temp_df["functionName"].str.contains("addliquidityeth"),
                    ["value_normal", "value"],
                ] = None

                temp_df = pd.concat(
                    [
                        temp_df,
                        temp_df[temp_df["functionName"].str.contains("removeliquidity")],
                    ]
                )

                liquidity_df = liquidity_df.drop(temp_df.index, axis=0)
                liquidity_df = pd.concat([liquidity_df, temp_df])
            else:
                temp_df["value"] = temp_df["value"].cumsum()
                temp_df["value_normal"] = temp_df["value_normal"].cumsum()
                temp_df.loc[
                    temp_df["functionName"].str.contains("addliquidityeth"),
                    ["value_normal", "value"],
                ] = None
                temp_df = pd.concat(
                    [
                        temp_df,
                        temp_df[temp_df["functionName"].str.contains("removeliquidity")],
                    ]
                )
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "value"
            ] *= [1, 0]
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "value_normal"
            ] *= [0, 1]
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "Fee"
            ] /= len(
                temp_df.loc[
                    temp_df["functionName"].str.contains("removeliquidity"), "Fee"
                ]
            )
            liquidity_df = liquidity_df.drop(temp_df.index, axis=0)
            liquidity_df = pd.concat([liquidity_df, temp_df])

        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("addliquidityeth"),
            ["Tag", "Notes"],
        ] = ["Movement", "Uniswap-V2 Deposit"]
        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("removeliquidity"),
            ["Tag", "Notes"],
        ] = ["Reward", "Uniswap-V2 Withdraw"]

        liquidity_df.loc[
            liquidity_df["value_normal"] < 0, "From Amount"
        ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
        liquidity_df.loc[
            liquidity_df["value_normal"] > 0, "To Amount"
        ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
        liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
        liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

        liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
            liquidity_df["value"] < 0, "value"
        ]
        liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
            liquidity_df["value"] > 0, "value"
        ]
        liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
            liquidity_df["value"] < 0, "tokenSymbol"
        ]
        liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
            liquidity_df["value"] > 0, "tokenSymbol"
        ]

        uniswap_out = pd.concat([uniswap_out, liquidity_df])

        # Function EXECUTE
        multicall = df[
            np.logical_or(
                df["functionName"].apply(lambda x: x.lower()).str.contains("execute"),
                df["functionName"].apply(lambda x: x.lower()).str.contains("swapExactTokensForETH".lower()),
            )
        ].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["value"] = eu.calculate_value_token(
            multicall.value, multicall.tokenDecimal
        )
        multicall["value_normal"] = eu.calculate_value_eth(
            multicall.value_normal.fillna(0)
        )
        multicall["value_internal"] = eu.calculate_value_eth(
            multicall.value_internal.fillna(0)
        )
        multicall.loc[multicall['from_internal'] == address, 'value_internal'] *= -1
        multicall.loc[multicall['from_normal'] == address, 'value_normal'] *= -1

        multicall["value_normal"] += multicall["value_internal"]
        multicall["value_normal"] = multicall["value_normal"].abs()

        multicall["from_internal"] = (
            multicall["from_internal"]
            .combine_first(multicall["from_normal"])
            .apply(lambda x: x.lower())
        )
        multicall["to_internal"] = (
            multicall["to_internal"]
            .combine_first(multicall["to_normal"])
            .apply(lambda x: x.lower())
        )

        multicall["from"] = multicall["from"].apply(lambda x: x.lower())
        multicall["to"] = multicall["to"].apply(lambda x: x.lower())

        count_df = multicall.groupby(multicall.index).agg({'hash': 'count'}).reset_index()
        multicall2 = pd.DataFrame()
        if any(count_df['hash'] == 2):
            count_df.index = count_df['timeStamp_normal']
            count_df = count_df.drop('timeStamp_normal', axis=1)
            multicall = pd.merge(count_df, multicall, left_on='timeStamp_normal', right_index=True)
            multicall2 = multicall[multicall['hash_x'] == 2].copy()
            multicall = multicall[multicall['hash_x'] == 1]

            multicall2['Fee'] /= 2
            multicall2.loc[multicall2['to'] != address, 'From Amount'] = -multicall2.loc[
                multicall2['to'] != address, 'value'].values
            multicall2.loc[multicall2['to'] == address, 'To Amount'] = multicall2.loc[
                multicall2['to'] == address, 'value'].values
            multicall2.loc[multicall2['to'] != address, 'From Coin'] = multicall2.loc[
                multicall2['to'] != address, 'tokenSymbol'].values
            multicall2.loc[multicall2['to'] == address, 'To Coin'] = multicall2.loc[
                multicall2['to'] == address, 'tokenSymbol'].values

            multicall2[['From Coin', 'From Amount']] = multicall2[['From Coin', 'From Amount']].bfill().infer_objects(
                copy=False)
            multicall2[['To Coin', 'To Amount']] = multicall2[['To Coin', 'To Amount']].ffill().infer_objects(
                copy=False)
            multicall2 = multicall2.drop_duplicates(subset=columns_out)
            multicall2["Tag"] = "Trade"
            multicall2["Notes"] = "Uniswap - execute"

        multicall.loc[multicall["to"] == address, "From Coin"] = gas_coin
        multicall.loc[multicall["to"] == address, "To Coin"] = multicall.loc[multicall["to"] == address, "tokenSymbol"]

        multicall.loc[multicall["from"] == address, "To Coin"] = gas_coin
        multicall.loc[multicall["from"] == address, "From Coin"] = multicall.loc[
            multicall["from"] == address, "tokenSymbol"]

        multicall.loc[multicall["to"] == address, "From Amount"] = -multicall.loc[
            multicall["to"] == address, "value_normal"]
        multicall.loc[multicall["to"] == address, "To Amount"] = multicall.loc[multicall["to"] == address, "value"]

        multicall.loc[multicall["from"] == address, "To Amount"] = multicall.loc[
            multicall["from"] == address, "value_normal"]
        multicall.loc[multicall["from"] == address, "From Amount"] = -multicall.loc[
            multicall["from"] == address, "value"]

        multicall["Fee"] = eu.calculate_gas(
            multicall.gasPrice, multicall.gasUsed_normal
        )

        multicall["Tag"] = "Trade"
        multicall["Notes"] = "Uniswap - Execute"

        if any(count_df['hash'] == 2):
            multicall = pd.concat([multicall, multicall2])

        uniswap_out = pd.concat([uniswap_out, multicall])
    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI UNISWAP SONO INCLUSE")

    uniswap_out = uniswap_out[[x for x in uniswap_out.columns if x in columns_out]]
    uniswap_out = uniswap_out.sort_index()

    uniswap_out = uniswap_out.drop_duplicates()

    return uniswap_out


def love(df, columns_out):
    df.index = df["timeStamp_normal"]
    df["value"] = eu.calculate_value_token(df["value"], df["tokenDecimal"])
    df["Fee"] = eu.calculate_gas(df["gasPrice"], df["gasUsed_normal"])

    # Claim
    df.loc[df["functionName"].str.contains("claim"), "To Coin"] = "LOVE"
    df.loc[df["functionName"].str.contains("claim"), "To Amount"] = df.loc[
        df["functionName"].str.contains("claim"), "value"
    ]
    df.loc[df["functionName"].str.contains("claim"), ["Tag", "Notes"]] = [
        "Reward",
        "LOVE Airdrop",
    ]

    # Deposit or withdraw from smart contract
    love_dep_with = df[
        np.logical_or(
            df["functionName"].str.contains("deposit"),
            df["functionName"].str.contains("withdraw"),
        )
    ]
    love_dep_with = love_dep_with[love_dep_with["tokenSymbol"] != "UNI-V2"].sort_index()
    love_dep_with["value"] = love_dep_with["value"].cumsum()
    love_dep_with.loc[
        love_dep_with["functionName"].str.contains("deposit"),
        ["To Coin", "Tag", "Notes"],
    ] = ["LOVE", "Movement", "Deposit LOVE"]
    love_dep_with.loc[love_dep_with["Notes"] == "Deposit LOVE", "To Amount"] = None
    love_dep_with.loc[
        love_dep_with["functionName"].str.contains("withdraw"),
        ["To Coin", "Tag", "Notes"],
    ] = ["LOVE", "Reward", "Withdraw LOVE"]
    love_dep_with["To Amount"] = love_dep_with["value"]

    df = pd.concat([df[df["functionName"].str.contains("claim")], love_dep_with])
    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    return df


def stargate(df, address, gas_coin, columns_out):
    df = df[
        df["functionName"] != "approve(address _spender, uint256 _value)"
        ]
    stargate_out = pd.DataFrame()
    if df.shape[0] > 0:
        df["value_normal"] = eu.calculate_value_eth(
            df["value_normal"]
        )
        df["value_internal"] = eu.calculate_value_eth(
            df["value_internal"].fillna(0)
        )
        df["value"] = eu.calculate_value_token(
            df["value"].fillna(0), df["tokenDecimal"].fillna(1)
        )
        df["Fee"] = eu.calculate_gas(
            df["gasPrice"], df["gasUsed_normal"]
        )
        df.index = df["timeStamp_normal"]

        df["functionName"] = df["functionName"].apply(
            lambda x: x.split("(")[0]
        )
        df.loc[
            df["functionName"] == "callBridgeCall", "functionName"
        ] = "swap"

        df.loc[
            df["functionName"] == "sendFrom", "functionName"
        ] = "swap"

        df.loc[
            df["functionName"] == "swapAndBridge", "functionName"
        ] = "swap"

        # Add Liquidity ERC20 or ETH
        liquidity = df.loc[
            df["functionName"].isin(["addLiquidityETH", "addLiquidity"])
        ].copy()
        df = pd.concat([df, liquidity]).drop_duplicates(keep=False)

        liquidity = liquidity[~liquidity["tokenSymbol"].str.contains("\*", na=False)]
        liquidity["Tag"] = "Movement"
        liquidity["Notes"] = "Stargate Add Liquidity"

        stargate_out = pd.concat([stargate_out, liquidity])

        # Stake S* Token
        stake_df = df[df["functionName"] == "deposit"].copy()
        df = pd.concat([df, stake_df]).drop_duplicates(keep=False)

        stake_df = stake_df[stake_df["value"] != 0].copy()
        stake_df.loc[stake_df["to"] == address, "To Amount"] = stake_df.loc[
            stake_df["to"] == address, "value"
        ]
        stake_df.loc[stake_df["to"] == address, "To Coin"] = stake_df.loc[
            stake_df["to"] == address, "tokenSymbol"
        ]
        stake_df.loc[stake_df["to"] == address, ["Tag", "Notes"]] = [
            "Reward",
            "Stargate Staking",
        ]
        stake_df["Tag"] = stake_df["Tag"].fillna("Movement")
        stake_df["Notes"] = stake_df["Notes"].fillna("Stargate Staking")

        stargate_out = pd.concat([stargate_out, stake_df])

        # withdraw S* Stake with rewards
        veSTG_addresses = ["0xD4888870C8686c748232719051b677791dBDa26D".lower()]

        withdraw_df = df.loc[df["functionName"] == "withdraw"].copy()
        df = pd.concat([df, withdraw_df]).drop_duplicates(keep=False)

        withdraw_df = withdraw_df[
            ~withdraw_df["tokenSymbol"].str.contains("\*", na=False)
        ]
        withdraw_df.loc[
            withdraw_df["from"].apply(lambda x: x.lower()).isin(veSTG_addresses),
            ["Tag", "Notes"],
        ] = ["Movement", "Stargate Withdraw Vesting"]
        withdraw_df.loc[
            ~withdraw_df["from"].apply(lambda x: x.lower()).isin(veSTG_addresses),
            ["Tag", "Notes"],
        ] = ["Reward", "Stargate Withdraw Staking"]
        withdraw_df.loc[
            ~withdraw_df["from"].apply(lambda x: x.lower()).isin(veSTG_addresses),
            ["To Amount", "To Coin"],
        ] = withdraw_df.loc[
            ~withdraw_df["from"].apply(lambda x: x.lower()).isin(veSTG_addresses),
            ["value", "tokenSymbol"],
        ].values

        stargate_out = pd.concat([stargate_out, withdraw_df])

        # Withdraw liquidity
        withdraw_df = df.loc[
            df["functionName"] == "instantRedeemLocal"
            ].copy()
        df = pd.concat([df, withdraw_df]).drop_duplicates(keep=False)

        withdraw_df = withdraw_df[
            ~withdraw_df["tokenSymbol"].str.contains("\*", na=False)
        ]
        withdraw_df["Tag"] = "Movement"
        withdraw_df["Notes"] = "Stargate Remove Liquidity"

        stargate_out = pd.concat([stargate_out, withdraw_df])

        # Bridging ETH
        temp_df = df.loc[df["functionName"] == "swapETH"].copy()
        df = pd.concat([df, temp_df]).drop_duplicates(keep=False)
        temp_df['From Amount'] = -temp_df['value_normal']
        temp_df['From Coin'] = gas_coin
        temp_df[['Tag', 'Notes']] = ['Movement', 'Stargate - Bridging swapETH']

        stargate_out = pd.concat([stargate_out, temp_df])

        # Bridging ERC20
        bridge_erc = df.loc[
            df["functionName"].isin(["swap", "", "sendTokens", "swapTokens"])
        ].copy()
        df = pd.concat([df, bridge_erc]).drop_duplicates(keep=False)

        bridge_erc["value_normal"] += bridge_erc["value_internal"]
        bridge_temp = bridge_erc.copy()
        bridge_temp["value_normal"] = None
        bridge_erc["value"] = None

        bridge_erc["From Amount"] = -bridge_erc["value_normal"]
        bridge_erc["From Coin"] = gas_coin

        bridge_temp["From Amount"] = -bridge_temp["value"]
        bridge_temp["From Coin"] = bridge_temp["tokenSymbol"]

        bridge_erc = pd.concat([bridge_temp, bridge_erc]).sort_index()
        bridge_erc["Fee"] /= 2

        bridge_erc["Noted"] = "Stargate - Bridging"
        bridge_erc["Tag"] = "Movement"

        stargate_out = pd.concat([stargate_out, bridge_erc])

        # STG vesting
        vesting = df.loc[
            df["functionName"].isin(
                ["create_lock", "increase_amount_and_time", "increase_unlock_time"]
            )
        ]
        df = pd.concat([df, vesting]).drop_duplicates(keep=False)

        vesting["Notes"] = "Stargate - STG vesting"
        vesting["Tag"] = "Movement"

        stargate_out = pd.concat([stargate_out, vesting])

        # Claim fees
        claim_df = df.loc[
            df["functionName"].isin(["claimTokens", "redeemFees"])
        ].copy()
        df = pd.concat([df, claim_df]).drop_duplicates(keep=False)

        claim_df[["To Coin", "To Amount"]] = claim_df[["tokenSymbol", "value"]].values
        claim_df[["Tag", "Notes"]] = ["Reward", "Stargate - Claim Fees"]

        stargate_out = pd.concat([stargate_out, claim_df])

    if df.shape[0] > 0:
        print("STARGATE TRANSACTIONS ARE NOT BEING CONSIDERED")

    stargate_out = stargate_out[[x for x in stargate_out.columns if x in columns_out]]
    stargate_out = stargate_out.sort_index()

    return stargate_out


def layer_zero_v2(df, gas_coin, columns_out):
    df["value_normal"] = eu.calculate_value_eth(df["value_normal"])
    df["value_internal"] = eu.calculate_value_eth(
        df["value_internal"].fillna(0)
    )
    df["value"] = eu.calculate_value_token(
        df["value"].fillna(0), df["tokenDecimal"].fillna(1)
    )
    df["Fee"] = eu.calculate_gas(
        df["gasPrice"], df["gasUsed_normal"]
    )
    df.index = df["timeStamp_internal"]

    df["value_normal"] += df["value_internal"]
    stargate_v2_temp = df.copy()
    stargate_v2_temp["value_normal"] = None
    df["value"] = None

    df["To Amount"] = df["value_normal"]
    df["To Coin"] = gas_coin

    stargate_v2_temp["To Amount"] = stargate_v2_temp["value"]
    stargate_v2_temp["To Coin"] = stargate_v2_temp["tokenSymbol"]

    df = pd.concat([stargate_v2_temp, df]).sort_index()
    df["Fee"] /= 2

    df["Noted"] = "Stargate - Layer Zero Relayer V2"
    df["Tag"] = "Movement"

    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    return df


def pancake(df, address, columns_out, gas_coin):
    pancake_out = pd.DataFrame()
    if df.shape[0] > 0:
        df["value_normal"] = eu.calculate_value_eth(
            df["value_normal"]
        )
        df["value_internal"] = eu.calculate_value_eth(
            df["value_internal"].fillna(0)
        )
        df["value"] = eu.calculate_value_token(
            df["value"].fillna(0), df["tokenDecimal"].fillna(1)
        )
        df["Fee"] = eu.calculate_gas(
            df["gasPrice"], df["gasUsed_normal"]
        )
        df.index = df["timeStamp_normal"]
        df["value_normal"] += df["value_internal"]

        # Add/Remove Cake into Cake Pools
        liquidity = df.loc[np.logical_or(df["functionName"].str.contains("deposit"),
                                         df["functionName"].str.contains("withdrawAll"))].copy()
        liquidity = liquidity[liquidity['tokenSymbol'] == 'Cake'].copy()
        df = pd.concat([df, liquidity]).drop_duplicates(keep=False)

        liquidity = liquidity.sort_index()

        liquidity.loc[liquidity['functionName'].str.contains("deposit"), 'value'] *= -1
        liquidity['value'] = liquidity['value'].cumsum()
        liquidity.loc[liquidity['value'] > 0, 'To Amount'] = liquidity.loc[liquidity['value'] > 0, 'value']
        liquidity.loc[liquidity['value'] > 0, 'To Coin'] = 'Cake'
        liquidity.loc[liquidity['value'] > 0, ['Tag', 'Notes']] = ['Reward', 'Pancake Swap - Cake Withdrawal']
        liquidity.loc[liquidity['value'] < 0, ['Tag', 'Notes']] = ['Movement', 'Pancake Swap - Cake Deposit']

        pancake_out = pd.concat([pancake_out, liquidity])

        # Migrate Cake Liquidity
        migration = df[df["functionName"].str.contains("migrateFromCakePool")].copy()
        df = pd.concat([df, migration]).drop_duplicates(keep=False)
        migration[['Tag', 'Notes']] = ['Movement', 'Pancake Swap - Liquidity Migration']
        pancake_out = pd.concat([pancake_out, migration])

        # Calling function multicall
        multicall = df[df["functionName"].str.contains("multicall")].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["from_internal"] = multicall["from_internal"].fillna('').apply(lambda x: x.lower())
        multicall["to_internal"] = multicall["to_internal"].fillna('').apply(lambda x: x.lower())

        multicall["from"] = multicall["from"].fillna('').apply(lambda x: x.lower())
        multicall["to"] = multicall["to"].fillna('').apply(lambda x: x.lower())

        multicall.loc[multicall["from_internal"] == address, "From Coin"] = gas_coin
        multicall.loc[multicall["from_internal"] == address, "To Coin"] = multicall.loc[
            multicall["from_internal"] == address, "tokenSymbol"]

        multicall.loc[multicall["to_internal"] == address, "To Coin"] = gas_coin
        multicall.loc[multicall["to_internal"] == address, "From Coin"] = multicall.loc[
            multicall["to_internal"] == address, "tokenSymbol"
        ]

        multicall.loc[
            multicall["from_internal"] == address, "From Amount"
        ] = -multicall.loc[multicall["from_normal"] == address, "value_normal"].values
        multicall.loc[
            multicall["from_internal"] == address, "To Amount"
        ] = multicall.loc[multicall["from_internal"] == address, "value"]

        multicall.loc[multicall["to_internal"] == address, "To Amount"] = multicall.loc[
            multicall["to_internal"] == address, "value_normal"
        ]
        multicall.loc[
            multicall["to_internal"] == address, "From Amount"
        ] = -multicall.loc[multicall["to_internal"] == address, "value"]

        # From ERC20 to ERC20
        multicall.loc[pd.isna(multicall['blockNumber_internal']), 'Fee'] /= 2
        multicall.loc[
            np.logical_and(pd.isna(multicall['blockNumber_internal']), multicall['to'] != address), 'From Amount'] = - \
            multicall.loc[
                np.logical_and(pd.isna(multicall['blockNumber_internal']), multicall['to'] != address), 'value'].values
        multicall.loc[
            np.logical_and(pd.isna(multicall['blockNumber_internal']), multicall['to'] == address), 'To Amount'] = \
            multicall.loc[
                np.logical_and(pd.isna(multicall['blockNumber_internal']), multicall['to'] == address), 'value'].values
        multicall.loc[
            np.logical_and(pd.isna(multicall['blockNumber_internal']), multicall['to'] != address), 'From Coin'] = \
            multicall.loc[np.logical_and(pd.isna(multicall['blockNumber_internal']),
                                         multicall['to'] != address), 'tokenSymbol'].values
        multicall.loc[
            np.logical_and(pd.isna(multicall['blockNumber_internal']), multicall['to'] == address), 'To Coin'] = \
            multicall.loc[np.logical_and(pd.isna(multicall['blockNumber_internal']),
                                         multicall['to'] == address), 'tokenSymbol'].values
        multicall = multicall.ffill().infer_objects(copy=False).bfill().infer_objects(copy=False)
        multicall = multicall.drop_duplicates(subset=columns_out)
        multicall["Tag"] = "Trade"
        multicall["Notes"] = "Pancake Swap - Swap"

        pancake_out = pd.concat([pancake_out, multicall])

        # Calling function swapExactTokensForTokens
        swaptokens = df[df["functionName"].str.contains("swapExactTokensForTokens")].copy()
        df = pd.concat([df, swaptokens]).drop_duplicates(keep=False)

        swaptokens["from_internal"] = swaptokens["from_internal"].fillna('').apply(lambda x: x.lower())
        swaptokens["to_internal"] = swaptokens["to_internal"].fillna('').apply(lambda x: x.lower())

        swaptokens["from"] = swaptokens["from"].fillna('').apply(lambda x: x.lower())
        swaptokens["to"] = swaptokens["to"].fillna('').apply(lambda x: x.lower())

        swaptokens.loc[pd.isna(swaptokens['blockNumber_internal']), 'Fee'] /= 2
        swaptokens.loc[
            np.logical_and(pd.isna(swaptokens['blockNumber_internal']), swaptokens['to'] != address), 'From Amount'] = - \
            swaptokens.loc[
                np.logical_and(pd.isna(swaptokens['blockNumber_internal']),
                               swaptokens['to'] != address), 'value'].values
        swaptokens.loc[
            np.logical_and(pd.isna(swaptokens['blockNumber_internal']), swaptokens['to'] == address), 'To Amount'] = \
            swaptokens.loc[
                np.logical_and(pd.isna(swaptokens['blockNumber_internal']),
                               swaptokens['to'] == address), 'value'].values
        swaptokens.loc[
            np.logical_and(pd.isna(swaptokens['blockNumber_internal']), swaptokens['to'] != address), 'From Coin'] = \
            swaptokens.loc[np.logical_and(pd.isna(swaptokens['blockNumber_internal']),
                                          swaptokens['to'] != address), 'tokenSymbol'].values
        swaptokens.loc[
            np.logical_and(pd.isna(swaptokens['blockNumber_internal']), swaptokens['to'] == address), 'To Coin'] = \
            swaptokens.loc[np.logical_and(pd.isna(swaptokens['blockNumber_internal']),
                                          swaptokens['to'] == address), 'tokenSymbol'].values
        swaptokens[['From Coin', 'From Amount']] = swaptokens[['From Coin', 'From Amount']].infer_objects(
            copy=False).ffill()
        swaptokens[['To Coin', 'To Amount']] = swaptokens[['To Coin', 'To Amount']].infer_objects(copy=False).bfill()
        swaptokens = swaptokens.drop_duplicates(subset=columns_out)
        swaptokens["Tag"] = "Trade"
        swaptokens["Notes"] = "Pancake Swap - swapExactTokensForTokens"

        pancake_out = pd.concat([pancake_out, swaptokens])

        # Calling function swapETHForExactTokens
        swapeth = df[np.logical_or(df["functionName"].str.contains("swapETHForExactTokens"),
                                   df["functionName"].str.contains("swapExactETHForTokens"))].copy()
        df = pd.concat([df, swapeth]).drop_duplicates(keep=False)

        swapeth["from_internal"] = swapeth["from_internal"].fillna('').apply(lambda x: x.lower())
        swapeth["to_internal"] = swapeth["to_internal"].fillna('').apply(lambda x: x.lower())

        swapeth["from"] = swapeth["from"].fillna('').apply(lambda x: x.lower())
        swapeth["to"] = swapeth["to"].fillna('').apply(lambda x: x.lower())

        swapeth.loc[swapeth["from_normal"] == address, "From Coin"] = gas_coin
        swapeth.loc[swapeth["from_normal"] == address, "To Coin"] = swapeth.loc[
            swapeth["from_normal"] == address, "tokenSymbol"]

        swapeth.loc[swapeth["to_normal"] == address, "To Coin"] = gas_coin
        swapeth.loc[swapeth["to_normal"] == address, "From Coin"] = swapeth.loc[
            swapeth["to_normal"] == address, "tokenSymbol"
        ]

        swapeth["value_normal"] -= swapeth["value_internal"]

        swapeth.loc[
            swapeth["from_normal"] == address, "From Amount"
        ] = -swapeth.loc[swapeth["from_normal"] == address, "value_normal"].values
        swapeth.loc[
            swapeth["from_normal"] == address, "To Amount"
        ] = swapeth.loc[swapeth["from_normal"] == address, "value"]

        swapeth.loc[swapeth["to_normal"] == address, "To Amount"] = swapeth.loc[
            swapeth["to_normal"] == address, "value_normal"
        ]
        swapeth.loc[
            swapeth["to_normal"] == address, "From Amount"
        ] = -swapeth.loc[swapeth["to_normal"] == address, "value"]

        swapeth["Tag"] = "Trade"
        swapeth["Notes"] = "Pancake Swap - swapETHForExactTokens"

        pancake_out = pd.concat([pancake_out, swapeth])

        # Calling swapExactTokensForETH
        swaptokens = df[df["functionName"].str.contains("swapExactTokensForETH")].copy()
        df = pd.concat([df, swaptokens]).drop_duplicates(keep=False)

        swaptokens["from_internal"] = swaptokens["from_internal"].fillna('').apply(lambda x: x.lower())
        swaptokens["to_internal"] = swaptokens["to_internal"].fillna('').apply(lambda x: x.lower())

        swaptokens["from"] = swaptokens["from"].fillna('').apply(lambda x: x.lower())
        swaptokens["to"] = swaptokens["to"].fillna('').apply(lambda x: x.lower())

        swaptokens.loc[swaptokens["from_internal"] == address, "From Coin"] = gas_coin
        swaptokens.loc[swaptokens["from_internal"] == address, "To Coin"] = swaptokens.loc[
            swaptokens["from_internal"] == address, "tokenSymbol"]

        swaptokens.loc[swaptokens["to_internal"] == address, "To Coin"] = gas_coin
        swaptokens.loc[swaptokens["to_internal"] == address, "From Coin"] = swaptokens.loc[
            swaptokens["to_internal"] == address, "tokenSymbol"
        ]

        swaptokens.loc[
            swaptokens["from_internal"] == address, "From Amount"
        ] = -swaptokens.loc[swaptokens["from_normal"] == address, "value_normal"].values
        swaptokens.loc[
            swaptokens["from_internal"] == address, "To Amount"
        ] = swaptokens.loc[swaptokens["from_internal"] == address, "value"]

        swaptokens.loc[swaptokens["to_internal"] == address, "To Amount"] = swaptokens.loc[
            swaptokens["to_internal"] == address, "value_normal"
        ]
        swaptokens.loc[
            swaptokens["to_internal"] == address, "From Amount"
        ] = -swaptokens.loc[swaptokens["to_internal"] == address, "value"]

        swaptokens[['Tag', 'Notes']] = ['Trade', 'Pancake Swap - swapExactTokensForETH']

        pancake_out = pd.concat([pancake_out, swaptokens])

        # Harvest Cake from syrup pool
        harvest = df[df["functionName"].str.contains("harvest")].copy()
        df = pd.concat([df, harvest]).drop_duplicates(keep=False)
        harvest[['Tag', 'Notes']] = ['Reward', 'Pancake Swap - Harvest Syrup Pool']
        harvest[['To Amount', 'To Coin']] = harvest[['value', 'tokenSymbol']]

        pancake_out = pd.concat([pancake_out, harvest])

        # Adding and removing liquidity in other pools
        liquidity = df[np.logical_or(df["functionName"].str.contains("addLiquidityETH"),
                                     df["functionName"].str.contains("removeLiquidityETH"))].copy()
        df = pd.concat([df, liquidity]).drop_duplicates(keep=False)

        liquidity = liquidity[liquidity['tokenSymbol'] != 'Cake-LP']
        liquidity.loc[liquidity["functionName"].str.contains("addLiquidityETH"), 'value_normal'] *= -1
        liquidity.loc[liquidity["functionName"].str.contains("addLiquidityETH"), 'value'] *= -1

        liquidity_out = pd.DataFrame()
        for token in liquidity['tokenSymbol'].unique():
            temp_df = liquidity[liquidity['tokenSymbol'] == token].copy()
            temp_df = temp_df.sort_index()
            temp_df['value_normal'] = temp_df['value_normal'].cumsum()
            temp_df['value'] = temp_df['value'].cumsum()
            temp_df.loc[temp_df["functionName"].str.contains("addLiquidityETH"), 'value_normal'] = None
            temp_df.loc[temp_df["functionName"].str.contains("addLiquidityETH"), 'value'] = None
            temp_df.loc[temp_df["functionName"].str.contains("removeLiquidityETH"), 'Fee'] /= 2
            temp_df = pd.concat([temp_df, temp_df.iloc[[-1], :]])

            if temp_df.iloc[-1, 40] > 0:
                temp_df.iloc[-1, 94] = temp_df.iloc[-1, 40]
                temp_df.iloc[-1, 92] = temp_df.iloc[-1, 42]
            elif temp_df.iloc[-1, 40] < 0:
                temp_df.iloc[-1, 93] = temp_df.iloc[-1, 40]
                temp_df.iloc[-1, 91] = temp_df.iloc[-1, 42]

            if temp_df.iloc[-2, 8] > 0:
                temp_df.iloc[-2, 94] = temp_df.iloc[-2, 8]
                temp_df.iloc[-2, 92] = 'BNB'
            elif temp_df.iloc[-2, 8] < 0:
                temp_df.iloc[-2, 93] = temp_df.iloc[-2, 8]
                temp_df.iloc[-2, 91] = 'BNB'

            temp_df.loc[temp_df['functionName'].str.contains("removeLiquidityETH"), ['Tag', 'Notes']] = ['Reward',
                                                                                                         'Pancake Swap - Remove liquidity']
            temp_df.loc[temp_df['functionName'].str.contains("addLiquidityETH"), ['Tag', 'Notes']] = ['Movement',
                                                                                                      'Pancake Swap - Add liquidity']

            liquidity_out = pd.concat([liquidity_out, temp_df])

    if df.shape[0] > 0:
        print("PANCAKE SWAP: TRANSAZIONI MANCANTI")

    pancake_out = pancake_out[[x for x in pancake_out.columns if x in columns_out]]
    pancake_out = pancake_out.sort_index()

    return pancake_out


def sushi(df, columns_out, gas_coin):
    sushi_out = pd.DataFrame()
    if df.shape[0] >= 0:
        df["value_normal"] = eu.calculate_value_eth(
            df["value_normal"]
        )
        df["value_internal"] = eu.calculate_value_eth(
            df["value_internal"].fillna(0)
        )
        df["value"] = eu.calculate_value_token(
            df["value"].fillna(0), df["tokenDecimal"].fillna(1)
        )
        df["Fee"] = eu.calculate_gas(
            df["gasPrice"], df["gasUsed_normal"]
        )
        df.index = df["timeStamp_normal"]
        df["value_normal"] += df["value_internal"]

        # Calling function cook Bridge
        cook = df.loc[df["functionName"].str.contains("cook")].copy()
        df = pd.concat([df, cook]).drop_duplicates(keep=False)

        cook_temp = cook.copy()
        cook_temp["value_normal"] = None
        cook["value"] = None

        cook["From Amount"] = -cook["value_normal"]
        cook["From Coin"] = gas_coin

        cook_temp["From Amount"] = -cook_temp["value"]
        cook_temp["From Coin"] = cook_temp["tokenSymbol"]

        cook = pd.concat([cook_temp, cook]).sort_index()
        cook["Fee"] /= 2

        cook["Noted"] = "Sushi - Cook Bridging"
        cook["Tag"] = "Movement"

        sushi_out = pd.concat([sushi_out, cook])

    if df.shape[0] > 0:
        print("PANCAKE SWAP: TRANSAZIONI MANCANTI")

    sushi_out = sushi_out[[x for x in sushi_out.columns if x in columns_out]]
    sushi_out = sushi_out.sort_index()

    return sushi_out


def one_inch(df, address, columns_out, gas_coin):
    one_out = pd.DataFrame()
    if df.shape[0] > 0:
        df.index = df['timeStamp']
        df["value_normal"] = eu.calculate_value_eth(
            df["value_normal"]
        )
        df["value_internal"] = eu.calculate_value_eth(
            df["value_internal"].fillna(0)
        )
        df["value"] = eu.calculate_value_token(
            df["value"].fillna(0), df["tokenDecimal"].fillna(1)
        )
        df["Fee"] = eu.calculate_gas(
            df["gasPrice"], df["gasUsed_normal"]
        )
        df.index = df["timeStamp_normal"]
        df["value_normal"] += df["value_internal"]

        df.loc[df['from_normal'] == address, 'From Coin'] = gas_coin
        df.loc[df['from_normal'] == address, 'To Coin'] = df.loc[
            df['from_normal'] == address, 'tokenSymbol']
        df.loc[df['from_normal'] == address, 'From Amount'] = -df.loc[
            df['from_normal'] == address, 'value_normal']
        df.loc[df['from_normal'] == address, 'To Amount'] = df.loc[
            df['from_normal'] == address, 'value']

        df.loc[df['to_normal'] == address, 'From Coin'] = df.loc[
            df['to_normal'] == address, 'tokenSymbol']
        df.loc[df['to_normal'] == address, 'To Coin'] = gas_coin
        df.loc[df['to_normal'] == address, 'From Amount'] = -df.loc[df['to_normal'] == address, 'value']
        df.loc[df['to_normal'] == address, 'To Amount'] = df.loc[
            df['to_normal'] == address, 'value_normal']

        df[['Tag', 'Notes']] = ['Trade', '1inch - Trade']
        one_out = pd.concat([one_out, df[~pd.isna(df['blockHash'])]])
        # Execute orders
        df = df[pd.isna(df['blockHash'])]
        if df.shape[0] > 0:
            df.loc[df['from'] == address, 'From Coin'] = df.loc[df['from'] == address, 'tokenSymbol']
            df.loc[df['from'] == address, 'From Amount'] = -df.loc[df['from'] == address, 'value']

            df.loc[df['to'] == address, 'To Coin'] = df.loc[df['to'] == address, 'tokenSymbol']
            df.loc[df['to'] == address, 'To Amount'] = df.loc[df['to'] == address, 'value']
            df['Fee'] = eu.calculate_gas(df['gasPrice_erc20'], df['gasUsed'])
            df['Fee'] /= 2

            df[['From Coin', 'From Amount']] = df[['From Coin', 'From Amount']].infer_objects(
                copy=False).ffill()
            df[['To Coin', 'To Amount']] = df[['To Coin', 'To Amount']].infer_objects(copy=False).bfill()
            df = df.drop_duplicates(subset=columns_out)
            df[['Tag', 'Notes']] = ['Trade', '1inch - Execute']
            one_out = pd.concat([one_out, df])

        one_out = one_out[[x for x in one_out.columns if x in columns_out]]
        one_out = one_out.sort_index()

        return one_out


def graph(df, columns_out):
    graph_out = pd.DataFrame()
    if df.shape[0] > 0:
        df["value_normal"] = eu.calculate_value_eth(
            df["value_normal"]
        )
        df["value_internal"] = eu.calculate_value_eth(
            df["value_internal"].fillna(0)
        )
        df["value"] = eu.calculate_value_token(
            df["value"].fillna(0), df["tokenDecimal"].fillna(1)
        )
        df["Fee"] = eu.calculate_gas(
            df["gasPrice"], df["gasUsed_normal"]
        )
        df.index = df["timeStamp_normal"]
        df["value_normal"] += df["value_internal"]

        # Multicall function
        multicall = df[df['functionName'].str.contains('multicall')].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall.loc[multicall['functionName'].str.contains('multicall'), ['Tag', 'Notes']] = ['Movement',
                                                                                                'The Graph - multicall']
        graph_out = pd.concat([graph_out, multicall])

        # Deposit/Withdraw from subgraphs
        subgraph = df[np.logical_or(df['functionName'].str.contains('delegate'),
                                    df['functionName'].str.contains('withdraw'))].copy()
        df = pd.concat([df, subgraph]).drop_duplicates(keep=False)
        subgraph = subgraph.sort_index()
        subgraph.loc[np.logical_and(subgraph['functionName'].str.contains('delegate'),
                                    ~subgraph['functionName'].str.contains('withdraw')), 'value'] *= -1
        subgraph['value'] = subgraph['value'].cumsum()

        subgraph.loc[np.logical_and(subgraph['functionName'].str.contains('delegate'),
                                    ~subgraph['functionName'].str.contains('withdraw')), ['Tag', 'Notes']] = [
            'Movement', 'The Graph - Delegate']

        subgraph.loc[
            np.logical_and(subgraph['functionName'].str.contains('withdraw'), subgraph['value'] > 0), 'To Amount'] = \
            subgraph.loc[
                np.logical_and(subgraph['functionName'].str.contains('withdraw'), subgraph['value'] > 0), 'value']
        subgraph.loc[np.logical_and(subgraph['functionName'].str.contains('withdraw'),
                                    subgraph['value'] > 0), 'To Coin'] = 'GRT'
        subgraph.loc[
            np.logical_and(subgraph['functionName'].str.contains('withdraw'), subgraph['value'] > 0), ['Tag',
                                                                                                       'Notes']] = [
            'Reward', 'The Graph - Withdraw Delegate']

        graph_out = pd.concat([graph_out, subgraph])

    if df.shape[0] > 0:
        print("THE GRAPH: TRANSAZIONI MANCANTI")

    graph_out = graph_out[[x for x in graph_out.columns if x in columns_out]]
    graph_out = graph_out.sort_index()

    return graph_out


def yearn(df, columns_out):
    df = df[~df['tokenSymbol'].str.contains('yv', na=False)]
    df.index = df['timeStamp_normal']
    df = df.sort_index()

    df['value'] = eu.calculate_value_token(df['value'].fillna(0), df['tokenDecimal'].fillna(0))
    df.loc[df['value'] == 0, 'value'] = None

    df.loc[df['functionName'].str.contains('deposit'), 'value'] *= -1
    df['value'] = df['value'].cumsum()
    df.loc[df['value'] > 0, 'To Amount'] = df.loc[df['value'] > 0, 'value']
    df.loc[df['value'] > 0, 'To Coin'] = df.loc[df['value'] > 0, 'tokenSymbol']
    df.loc[df['value'] > 0, ['Tag', 'Notes']] = ['Reward', 'Yearn Finance - Withdraw']
    df['Tag'] = df['Tag'].fillna('Movement')
    df['Notes'] = df['Notes'].fillna('Yearn Finance - Deposit')

    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    return df


def zerox(df, address, gas_coin, columns_out):
    zero_out = pd.DataFrame()
    df["value"] = eu.calculate_value_token(df.value, df.tokenDecimal)
    df["value_normal"] = eu.calculate_value_eth(df.value_normal)
    df["value_internal"] = eu.calculate_value_eth(df.value_internal)
    df["value_normal"] += df["value_internal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df['to_internal'] = df['to_internal'].combine_first(df['to_normal'])
    df['from_internal'] = df['from_internal'].combine_first(df['from_normal'])
    df.index = df['timeStamp_normal']

    # Transform ERC20 function
    transform = df[df['functionName'].str.contains('transformERC20')].copy()
    df = pd.concat([transform, df]).drop_duplicates(keep=False)

    transform.loc[transform['to_internal'] == address, 'To Coin'] = gas_coin
    transform.loc[transform['to_internal'] == address, 'To Amount'] = transform.loc[
        transform['to_internal'] == address, 'value_normal']
    transform.loc[transform['to_internal'] == address, 'From Coin'] = transform.loc[
        transform['to_internal'] == address, 'tokenSymbol']
    transform.loc[transform['to_internal'] == address, 'From Amount'] = -transform.loc[
        transform['to_internal'] == address, 'value']

    transform.loc[transform['from_internal'] == address, 'From Coin'] = gas_coin
    transform.loc[transform['from_internal'] == address, 'From Amount'] = -transform.loc[
        transform['from_internal'] == address, 'value_normal']
    transform.loc[transform['from_internal'] == address, 'To Coin'] = transform.loc[
        transform['from_internal'] == address, 'tokenSymbol']
    transform.loc[transform['from_internal'] == address, 'To Amount'] = transform.loc[
        transform['from_internal'] == address, 'value']

    zero_out = pd.concat([zero_out, transform])

    if df.shape[0] > 0:
        print("0x - TRANSAZIONI MANCANTI")
    zero_out['Tag'] = zero_out['Tag'].fillna('Trade')
    zero_out['Notes'] = zero_out['Notes'].fillna('0x - Transform ERC20')

    zero_out = zero_out[[x for x in zero_out.columns if x in columns_out]]
    zero_out = zero_out.sort_index()

    return zero_out


def quick_swap(df, address, columns_out, gas_coin):
    quick_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df['functionName'] = df['functionName'].apply(lambda x: x.lower())
    if df.shape[0] > 0:
        # Adding and removing liquidity with ETH
        liquidity_df = df[
            np.logical_or(
                df["functionName"].str.contains("addliquidityeth"),
                df["functionName"].str.contains("removeliquidity"),
            )
        ].copy()
        df = pd.concat([df, liquidity_df]).drop_duplicates(keep=False)

        liquidity_df = liquidity_df[liquidity_df["tokenSymbol"] != "UNI-V2"]

        liquidity_df["Fee"] = eu.calculate_gas(
            liquidity_df.gasPrice, liquidity_df.gasUsed_normal
        )

        liquidity_df["value"] = eu.calculate_value_token(
            liquidity_df.value, liquidity_df.tokenDecimal
        )
        liquidity_df["value_normal"] = eu.calculate_value_eth(
            liquidity_df.value_normal.fillna(0)
        )
        liquidity_df["value_internal"] = eu.calculate_value_eth(
            liquidity_df.value_internal.fillna(0)
        )
        liquidity_df["value_normal"] += liquidity_df["value_internal"]

        liquidity_df.loc[
            pd.isna(liquidity_df["to_internal"]), "to_internal"
        ] = liquidity_df.loc[pd.isna(liquidity_df["to_internal"]), "to_normal"]

        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("addliquidityeth"),
            ["value_normal", "value"],
        ] *= -1
        liquidity_df = liquidity_df.sort_index()

        for token in liquidity_df["tokenSymbol"].unique():
            temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token]
            temp_df["value"] = temp_df["value"].cumsum()
            temp_df["value_normal"] = temp_df["value_normal"].cumsum()
            temp_df.loc[
                temp_df["functionName"].str.contains("addliquidityeth"),
                ["value_normal", "value"],
            ] = None
            temp_df = pd.concat(
                [
                    temp_df,
                    temp_df[temp_df["functionName"].str.contains("removeliquidity")],
                ]
            )
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "value"
            ] *= [1, 0]
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "value_normal"
            ] *= [0, 1]
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "Fee"
            ] /= len(
                temp_df.loc[
                    temp_df["functionName"].str.contains("removeliquidity"), "Fee"
                ]
            )
            liquidity_df = liquidity_df.drop(temp_df.index, axis=0)
            liquidity_df = pd.concat([liquidity_df, temp_df])

        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("addliquidityeth"),
            ["Tag", "Notes"],
        ] = ["Movement", "Quick Swap-V2 Deposit"]
        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("removeliquidity"),
            ["Tag", "Notes"],
        ] = ["Reward", "Quick Swap-V2 Withdraw"]

        liquidity_df.loc[
            liquidity_df["value_normal"] < 0, "From Amount"
        ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
        liquidity_df.loc[
            liquidity_df["value_normal"] > 0, "To Amount"
        ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
        liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
        liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

        liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
            liquidity_df["value"] < 0, "value"
        ]
        liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
            liquidity_df["value"] > 0, "value"
        ]
        liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
            liquidity_df["value"] < 0, "tokenSymbol"
        ]
        liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
            liquidity_df["value"] > 0, "tokenSymbol"
        ]

        quick_out = pd.concat([quick_out, liquidity_df])

        # Function swap
        multicall = df[
            np.logical_or(
                df["functionName"].str.contains("execute"),
                df["functionName"].str.contains("swapexactethfortokens"),
            )
        ].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["value"] = eu.calculate_value_token(
            multicall.value, multicall.tokenDecimal
        )
        multicall["value_normal"] = eu.calculate_value_eth(
            multicall.value_normal.fillna(0)
        )
        multicall["value_internal"] = eu.calculate_value_eth(
            multicall.value_internal.fillna(0)
        )
        multicall["value_normal"] += multicall["value_internal"]

        multicall["from_internal"] = (
            multicall["from_internal"]
            .combine_first(multicall["from_normal"])
            .apply(lambda x: x.lower())
        )
        multicall["to_internal"] = (
            multicall["to_internal"]
            .combine_first(multicall["to_normal"])
            .apply(lambda x: x.lower())
        )

        multicall["from"] = multicall["from"].apply(lambda x: x.lower())
        multicall["to"] = multicall["to"].apply(lambda x: x.lower())

        count_df = multicall.groupby(multicall.index).agg({'hash': 'count'}).reset_index()
        if any(count_df['hash'] == 2):
            count_df.index = count_df['timeStamp_normal']
            count_df = count_df.drop('timeStamp_normal', axis=1)
            multicall = pd.merge(count_df, multicall, left_on='timeStamp_normal', right_index=True)
            multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['to'] == address), 'To Amount'] = \
                multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['to'] == address), 'value']
            multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['to'] == address), 'To Coin'] = \
                multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['to'] == address), 'tokenSymbol']
            multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['from'] == address), 'From Amount'] = - \
                multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['from'] == address), 'value']
            multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['from'] == address), 'From Coin'] = \
                multicall.loc[np.logical_and(multicall['hash_x'] == 2, multicall['from'] == address), 'tokenSymbol']
            multicall.loc[multicall['hash_x'] == 2, ['To Coin', 'To Amount']] = multicall.loc[
                multicall['hash_x'] == 2, ['To Coin', 'To Amount']].ffill()
            multicall.loc[multicall['hash_x'] == 2, ['From Coin', 'From Amount']] = multicall.loc[
                multicall['hash_x'] == 2, ['From Coin', 'From Amount']].bfill()
            temp1 = multicall[multicall['hash_x'] == 2].drop_duplicates(subset=columns_out)
            multicall = multicall[multicall['hash_x'] != 2]
            multicall = pd.concat([multicall, temp1])
            multicall = multicall.drop('hash_x', axis=1)

        multicall.loc[multicall["from_internal"] == address, "From Coin"] = gas_coin
        multicall.loc[multicall["from_internal"] == address, "To Coin"] = multicall.loc[
            multicall["from_internal"] == address, "tokenSymbol"
        ]

        multicall.loc[multicall["to_internal"] == address, "To Coin"] = gas_coin
        multicall.loc[multicall["to_internal"] == address, "From Coin"] = multicall.loc[
            multicall["to_internal"] == address, "tokenSymbol"
        ]

        multicall.loc[
            multicall["from_internal"] == address, "From Amount"
        ] = -multicall.loc[multicall["from_internal"] == address, "value_normal"]
        multicall.loc[
            multicall["from_internal"] == address, "To Amount"
        ] = multicall.loc[multicall["from_internal"] == address, "value"]

        multicall.loc[multicall["to_internal"] == address, "To Amount"] = multicall.loc[
            multicall["to_internal"] == address, "value_normal"
        ]
        multicall.loc[
            multicall["to_internal"] == address, "From Amount"
        ] = -multicall.loc[multicall["to_internal"] == address, "value"]

        multicall["Fee"] = eu.calculate_gas(
            multicall.gasPrice, multicall.gasUsed_normal
        )

        multicall["Tag"] = "Quick Swap - Execute"
        multicall["Notes"] = "Trade"

        quick_out = pd.concat([quick_out, multicall])
    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI UNISWAP SONO INCLUSE")

    quick_out = quick_out[[x for x in quick_out.columns if x in columns_out]]
    quick_out = quick_out.sort_index()

    quick_out = quick_out.drop_duplicates()

    return quick_out


def metamask(df, address, columns_out):
    metamask_out = pd.DataFrame()

    df["value"] = eu.calculate_value_token(df["value"].fillna(0),
                                           df["tokenDecimal"].fillna(1))
    df["Fee"] = eu.calculate_gas(df["gasPrice"], df["gasUsed_normal"])
    df.index = df["timeStamp_normal"]

    # From ERC20 to ERC20
    swap = df[df['functionName'].str.contains('swap')].copy()
    df = pd.concat([df, swap]).drop_duplicates(keep=False)

    swap['Fee'] /= 2
    swap.loc[swap['to'] != address, 'From Amount'] = -swap.loc[swap['to'] != address, 'value'].values
    swap.loc[swap['to'] == address, 'To Amount'] = swap.loc[swap['to'] == address, 'value'].values
    swap.loc[swap['to'] != address, 'From Coin'] = swap.loc[swap['to'] != address, 'tokenSymbol'].values
    swap.loc[swap['to'] == address, 'To Coin'] = swap.loc[swap['to'] == address, 'tokenSymbol'].values

    swap = swap.ffill().infer_objects(copy=False).bfill().infer_objects(copy=False)
    swap = swap.drop_duplicates(subset=columns_out)
    swap["Tag"] = "Trade"
    swap["Notes"] = "Metamask - Swap"
    metamask_out = pd.concat([metamask_out, swap])

    if df.shape[0] > 0:
        print("ATTENZIONE - METAMAKS SWAPS NON INCLUSI")

    metamask_out = metamask_out[[x for x in metamask_out.columns if x in columns_out]]
    metamask_out = metamask_out.sort_index()

    return metamask_out


def ferro(df, address, columns_out):
    ferro_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    if df.shape[0] > 0:
        # Function swap
        multicall = df[df["functionName"].str.contains("swap")].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["value"] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)

        multicall["from"] = multicall["from"].fillna('').apply(lambda x: x.lower())
        multicall["to"] = multicall["to"].fillna('').apply(lambda x: x.lower())

        multicall.loc[multicall["to"] == address, "To Coin"] = multicall.loc[multicall["to"] == address, "tokenSymbol"]
        multicall.loc[multicall["to"] == address, "To Amount"] = multicall.loc[multicall["to"] == address, "value"]

        multicall.loc[multicall["from"] == address, "From Coin"] = multicall.loc[
            multicall["from"] == address, "tokenSymbol"]
        multicall.loc[multicall["from"] == address, "From Amount"] = [-x for x in multicall.loc[
            multicall["from"] == address, "value"]]

        multicall["Fee"] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

        multicall[['From Coin', 'From Amount']] = multicall[['From Coin', 'From Amount']].ffill().infer_objects(
            copy=False)
        multicall[['To Coin', 'To Amount']] = multicall[['To Coin', 'To Amount']].bfill().infer_objects(copy=False)
        multicall = multicall.drop_duplicates(subset=columns_out)
        multicall["Tag"] = "Trade"
        multicall["Notes"] = "Ferro - swap"

        ferro_out = pd.concat([ferro_out, multicall])

        # Adding and removing liquidity with ETH
        liquidity_df = df[
            np.logical_or(
                df["functionName"].apply(lambda x: x.lower()).str.contains("addliquidity"),
                df["functionName"].apply(lambda x: x.lower()).str.contains("removeliquidity"),
            )
        ].copy()

        df = pd.concat([df, liquidity_df]).drop_duplicates(keep=False)

        liquidity_df["functionName"] = liquidity_df["functionName"].apply(lambda x: x.lower())

        liquidity_df["Fee"] = eu.calculate_gas(
            liquidity_df.gasPrice, liquidity_df.gasUsed_normal
        )

        liquidity_df["value"] = eu.calculate_value_token(
            liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
        )
        liquidity_df["value_normal"] = eu.calculate_value_eth(
            liquidity_df.value_normal.fillna(0)
        )
        liquidity_df["value_internal"] = eu.calculate_value_eth(
            liquidity_df.value_internal.fillna(0)
        )
        liquidity_df["value_normal"] += liquidity_df["value_internal"]

        liquidity_df.loc[
            pd.isna(liquidity_df["to_internal"]), "to_internal"
        ] = liquidity_df.loc[pd.isna(liquidity_df["to_internal"]), "to_normal"]

        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("addliquidityeth"),
            ["value_normal", "value"],
        ] *= -1
        liquidity_df = liquidity_df.sort_index()

        for token in liquidity_df["tokenSymbol"].unique():
            temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
            liquidity_df = pd.concat([liquidity_df, temp_df]).drop_duplicates(keep=False)

            temp_df.loc[temp_df["functionName"].str.contains("addliquidity"), "value"] *= -1

            temp_df["value"] = temp_df["value"].cumsum()
            temp_df.loc[temp_df["functionName"].str.contains("addliquidity"), "value"] = None

            temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']
            temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']

            temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']
            temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']

            liquidity_df = pd.concat([liquidity_df, temp_df])

        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("addliquidity"),
            ["Tag", "Notes"],
        ] = ["Movement", "Ferro - Deposit"]
        liquidity_df.loc[
            liquidity_df["functionName"].str.contains("removeliquidity"),
            ["Tag", "Notes"],
        ] = ["Reward", "Ferro - Withdraw"]

        liquidity_df['Fee'] /= 2

        ferro_out = pd.concat([ferro_out, liquidity_df])

        # Staking LP
        stake_df = df[
            np.logical_or(
                df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"),
                df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"),
            )
        ].copy()

        df = pd.concat([df, stake_df]).drop_duplicates(keep=False)

        stake_df = stake_df[stake_df['tokenSymbol'] != 'BOOST']
        stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"), ['Tag', 'Notes']] = [
            'Movement', 'Ferro - Deposit stake']

        stake_df.loc[np.logical_and(stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"),
                                    ~pd.isna(stake_df['value'])), 'Tag'] = 'Reward'

        stake_df.loc[np.logical_and(stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"),
                                    ~pd.isna(stake_df['value'])), 'To Coin'] = 'FER'

        stake_df.loc[np.logical_and(stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"),
                                    ~pd.isna(stake_df['value'])), 'To Amount'] = eu.calculate_value_token(stake_df.loc[
                                                                                                              np.logical_and(
                                                                                                                  stake_df[
                                                                                                                      "functionName"].apply(
                                                                                                                      lambda
                                                                                                                          x: x.lower()).str.contains(
                                                                                                                      "deposit"),
                                                                                                                  ~pd.isna(
                                                                                                                      stake_df[
                                                                                                                          'value'])), 'value'],
                                                                                                          stake_df.loc[
                                                                                                              np.logical_and(
                                                                                                                  stake_df[
                                                                                                                      "functionName"].apply(
                                                                                                                      lambda
                                                                                                                          x: x.lower()).str.contains(
                                                                                                                      "deposit"),
                                                                                                                  ~pd.isna(
                                                                                                                      stake_df[
                                                                                                                          'value'])), 'tokenDecimal'])

        stake_df['value'] = eu.calculate_value_token(stake_df['value'].fillna(0), stake_df['tokenDecimal'].fillna(0))
        stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), ['Tag', 'Notes',
                                                                                                    'To Coin']] = [
            'Reward', 'Ferro - Withdraw stake', 'FER']
        stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), 'To Amount'] = \
            stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), 'value']
        stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), 'Fee'] /= 2

        ferro_out = pd.concat([ferro_out, stake_df])

        if df.shape[0] > 0:
            print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI FERRO SONO INCLUSE")

        ferro_out = ferro_out[[x for x in ferro_out.columns if x in columns_out]]
        ferro_out = ferro_out.sort_index()

        ferro_out = ferro_out.drop_duplicates()

        return ferro_out


def mm_finance(df, address, columns_out, gas_coin):
    mm_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Function multicall V2
    multicall = df[np.logical_or(df["functionName"].str.contains("swapExactETHForTokens"),
                                 df["functionName"].str.contains("swapExactTokensForETH"))].copy()
    df = pd.concat([df, multicall]).drop_duplicates(keep=False)

    multicall["value"] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)
    multicall["value_normal"] = eu.calculate_value_eth(multicall.value_normal)
    multicall["value_internal"] = eu.calculate_value_eth(multicall.value_internal)

    multicall.loc[multicall['from_internal'] == address, 'value_internal'] *= -1
    multicall.loc[multicall['from_normal'] == address, 'value_normal'] *= -1

    multicall["value_normal"] += multicall["value_internal"]
    multicall["value_normal"] = multicall["value_normal"].abs()

    multicall["from_normal"] = multicall["from_normal"].combine_first(
        multicall["from_internal"].fillna('').apply(lambda x: x.lower()))
    multicall["to_normal"] = multicall["to_normal"].combine_first(
        multicall["to_internal"].fillna('').apply(lambda x: x.lower()))

    multicall["from"] = multicall["from"].fillna('').apply(lambda x: x.lower())
    multicall["to"] = multicall["to"].fillna('').apply(lambda x: x.lower())

    multicall.loc[multicall["to"] == address, "From Coin"] = gas_coin
    multicall.loc[multicall["to"] == address, "To Coin"] = multicall.loc[
        multicall["to"] == address, "tokenSymbol"]

    multicall.loc[multicall["from"] == address, "To Coin"] = gas_coin
    multicall.loc[multicall["from"] == address, "From Coin"] = multicall.loc[
        multicall["from"] == address, "tokenSymbol"]

    multicall.loc[multicall["to"] == address, "From Amount"] = -multicall.loc[
        multicall["to"] == address, "value_normal"]
    multicall.loc[multicall["to"] == address, "To Amount"] = multicall.loc[multicall["to"] == address, "value"]

    multicall.loc[multicall["from"] == address, "To Amount"] = multicall.loc[
        multicall["from"] == address, "value_normal"]
    multicall.loc[multicall["from"] == address, "From Amount"] = -multicall.loc[
        multicall["from"] == address, "value"]

    multicall["Fee"] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

    multicall["Tag"] = "Trade"
    multicall["Notes"] = "MM Finance - swapExactTokensForETH"

    mm_out = pd.concat([mm_out, multicall])

    # Adding and removing liquidity with ETH
    liquidity_df = df[
        np.logical_or(
            df["functionName"].apply(lambda x: x.lower()).str.contains("addliquidityeth"),
            df["functionName"].apply(lambda x: x.lower()).str.contains("removeliquidityeth"),
        )
    ].copy()

    df = pd.concat([df, liquidity_df]).drop_duplicates(keep=False)

    liquidity_df["functionName"] = liquidity_df["functionName"].apply(lambda x: x.lower())

    liquidity_df["Fee"] = eu.calculate_gas(
        liquidity_df.gasPrice, liquidity_df.gasUsed_normal
    )

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        pd.isna(liquidity_df["to_internal"]), "to_internal"
    ] = liquidity_df.loc[pd.isna(liquidity_df["to_internal"]), "to_normal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("addliquidityeth"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("addliquidityeth"),
            ["value_normal", "value"],
        ] = None
        temp_df = pd.concat(
            [
                temp_df,
                temp_df[temp_df["functionName"].str.contains("removeliquidity")],
            ]
        )
        temp_df.loc[
            temp_df["functionName"].str.contains("removeliquidity"), "value"
        ] *= [1, 0]
        temp_df.loc[
            temp_df["functionName"].str.contains("removeliquidity"), "value_normal"
        ] *= [0, 1]
        temp_df.loc[
            temp_df["functionName"].str.contains("removeliquidity"), "Fee"
        ] /= len(
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "Fee"
            ]
        )
        liquidity_df = liquidity_df.drop(temp_df.index, axis=0)
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("addliquidityeth"),
        ["Tag", "Notes"],
    ] = ["Movement", "MM Finance - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("removeliquidity"),
        ["Tag", "Notes"],
    ] = ["Reward", "MM Finance - Withdraw"]

    liquidity_df.loc[
        liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[
        liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    mm_out = pd.concat([mm_out, liquidity_df])

    # Staking LP
    stake_df = df[
        np.logical_or(
            df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"),
            df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"),
        )
    ].copy()

    df = pd.concat([df, stake_df]).drop_duplicates(keep=False)

    stake_df["value"] = eu.calculate_value_token(
        stake_df.value.fillna(0), stake_df.tokenDecimal.fillna(0)
    )

    stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"), 'To Coin'] = stake_df.loc[
        stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"), 'tokenSymbol']
    stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"), 'To Amount'] = \
        stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"), 'value']

    stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"), ['Tag', 'Notes']] = [
        'Movement', 'MM Finance - Deposit stake']

    stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), ['Tag', 'Notes',
                                                                                                'To Coin']] = [
        'Reward', 'MM Finance - Withdraw stake', 'MMF']
    stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), 'To Amount'] = \
        stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), 'value']
    stake_df.loc[stake_df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"), 'Fee'] /= 2

    mm_out = pd.concat([mm_out, stake_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI MM FINANCE SONO INCLUSE")

    mm_out = mm_out[[x for x in mm_out.columns if x in columns_out]]
    mm_out = mm_out.sort_index()

    mm_out = mm_out.drop_duplicates()

    return mm_out


def argo(df, columns_out):
    argo_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    if df.shape[0] > 0:
        # stake
        multicall = df[df["functionName"].str.contains("stake")].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["value"] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)

        multicall["To Coin"] = "bCRO"
        multicall["From Coin"] = 'CRO'

        multicall["value_normal"] = eu.calculate_value_eth(multicall["value_normal"])
        multicall["From Amount"] = [-x for x in multicall["value_normal"]]
        multicall["To Amount"] = multicall["value"]

        multicall["Tag"] = "Trade"
        multicall["Notes"] = "Argo - Liquid Staking"

        argo_out = pd.concat([argo_out, multicall])

        # Contract interaction
        df = df[df['tokenSymbol'] != 'xARGO']
        df["To Amount"] = eu.calculate_value_token(df.value, df.tokenDecimal)
        df["To Coin"] = df['tokenSymbol']
        df[["Tag", "Notes"]] = ['Reward', 'Argo - Interaction']

        argo_out = pd.concat([argo_out, df])

        argo_out = argo_out[[x for x in argo_out.columns if x in columns_out]]
        argo_out = argo_out.sort_index()

        return argo_out


def vvs(df, address, columns_out, gas_coin):
    vvs_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Function multicall V2
    multicall = df[np.logical_or(df["functionName"].str.contains("swapExactETHForTokens"),
                                 df["functionName"].str.contains("swapExactTokensForETH"))].copy()
    df = pd.concat([df, multicall]).drop_duplicates(keep=False)

    multicall["value"] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)
    multicall["value_normal"] = eu.calculate_value_eth(multicall.value_normal)
    multicall["value_internal"] = eu.calculate_value_eth(multicall.value_internal)

    multicall.loc[multicall['from_internal'] == address, 'value_internal'] *= -1
    multicall.loc[multicall['from_normal'] == address, 'value_normal'] *= -1

    multicall["value_normal"] += multicall["value_internal"]
    multicall["value_normal"] = multicall["value_normal"].abs()

    multicall["from_normal"] = multicall["from_normal"].combine_first(
        multicall["from_internal"].fillna('').apply(lambda x: x.lower()))
    multicall["to_normal"] = multicall["to_normal"].combine_first(
        multicall["to_internal"].fillna('').apply(lambda x: x.lower()))

    multicall["from"] = multicall["from"].fillna('').apply(lambda x: x.lower())
    multicall["to"] = multicall["to"].fillna('').apply(lambda x: x.lower())

    multicall.loc[multicall["to"] == address, "From Coin"] = gas_coin
    multicall.loc[multicall["to"] == address, "To Coin"] = multicall.loc[
        multicall["to"] == address, "tokenSymbol"]

    multicall.loc[multicall["from"] == address, "To Coin"] = gas_coin
    multicall.loc[multicall["from"] == address, "From Coin"] = multicall.loc[
        multicall["from"] == address, "tokenSymbol"]

    multicall.loc[multicall["to"] == address, "From Amount"] = -multicall.loc[
        multicall["to"] == address, "value_normal"]
    multicall.loc[multicall["to"] == address, "To Amount"] = multicall.loc[multicall["to"] == address, "value"]

    multicall.loc[multicall["from"] == address, "To Amount"] = multicall.loc[
        multicall["from"] == address, "value_normal"]
    multicall.loc[multicall["from"] == address, "From Amount"] = -multicall.loc[
        multicall["from"] == address, "value"]

    multicall["Fee"] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

    multicall["Tag"] = "Trade"
    multicall["Notes"] = "VVS Finance - swapExactTokensForETH"

    vvs_out = pd.concat([vvs_out, multicall])

    # Adding and removing liquidity with ETH
    liquidity_df = df[
        np.logical_or(
            df["functionName"].apply(lambda x: x.lower()).str.contains("addliquidityeth"),
            df["functionName"].apply(lambda x: x.lower()).str.contains("removeliquidityeth"),
        )
    ].copy()

    df = pd.concat([df, liquidity_df]).drop_duplicates(keep=False)

    liquidity_df["functionName"] = liquidity_df["functionName"].apply(lambda x: x.lower())

    liquidity_df["Fee"] = eu.calculate_gas(
        liquidity_df.gasPrice, liquidity_df.gasUsed_normal
    )

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("addliquidityeth"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()
    liquidity_df = liquidity_df[liquidity_df['tokenSymbol'] != 'VVS-LP']

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("addliquidityeth"),
            ["value_normal", "value"],
        ] = None
        temp_df = pd.concat(
            [
                temp_df,
                temp_df[temp_df["functionName"].str.contains("removeliquidity")],
            ]
        )
        temp_df.loc[
            temp_df["functionName"].str.contains("removeliquidity"), "value"
        ] *= [1, 0]
        temp_df.loc[
            temp_df["functionName"].str.contains("removeliquidity"), "value_normal"
        ] *= [0, 1]
        temp_df.loc[
            temp_df["functionName"].str.contains("removeliquidity"), "Fee"
        ] /= len(
            temp_df.loc[
                temp_df["functionName"].str.contains("removeliquidity"), "Fee"
            ]
        )
        liquidity_df = liquidity_df.drop(temp_df.index, axis=0)
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("addliquidityeth"),
        ["Tag", "Notes"],
    ] = ["Movement", "MM Finance - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("removeliquidity"),
        ["Tag", "Notes"],
    ] = ["Reward", "MM Finance - Withdraw"]

    liquidity_df.loc[
        liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[
        liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    vvs_out = pd.concat([vvs_out, liquidity_df])

    # Staking LP
    stake_df = df[
        np.logical_or(
            df["functionName"].apply(lambda x: x.lower()).str.contains("withdraw"),
            df["functionName"].apply(lambda x: x.lower()).str.contains("deposit"),
        )
    ].copy()

    df = pd.concat([df, stake_df]).drop_duplicates(keep=False)

    stake_df[['Tag', 'Notes']] = ['Movement', 'VVS Finance - Stake']
    vvs_out = pd.concat([vvs_out, stake_df.drop_duplicates()])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI VVS FINANCE SONO INCLUSE")

    vvs_out = vvs_out[[x for x in vvs_out.columns if x in columns_out]]
    vvs_out = vvs_out.sort_index()

    return vvs_out


def sofi_swap(df, address, columns_out, gas_coin):
    sofi_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Function multicall V2
    multicall = df[np.logical_or(df["functionName"].str.contains("swapETHForExactTokens"),
                                 df["functionName"].str.contains("swapExactTokensForETH"))].copy()
    df = pd.concat([df, multicall]).drop_duplicates(keep=False)

    multicall["value"] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)
    multicall["value_normal"] = eu.calculate_value_eth(multicall.value_normal)
    multicall["value_internal"] = eu.calculate_value_eth(multicall.value_internal)

    multicall.loc[multicall['from_internal'] == address, 'value_internal'] *= -1
    multicall.loc[multicall['from_normal'] == address, 'value_normal'] *= -1

    multicall["value_normal"] += multicall["value_internal"]
    multicall["value_normal"] = multicall["value_normal"].abs()

    multicall["from_normal"] = multicall["from_normal"].combine_first(
        multicall["from_internal"].fillna('').apply(lambda x: x.lower()))
    multicall["to_normal"] = multicall["to_normal"].combine_first(
        multicall["to_internal"].fillna('').apply(lambda x: x.lower()))

    multicall["from"] = multicall["from"].fillna('').apply(lambda x: x.lower())
    multicall["to"] = multicall["to"].fillna('').apply(lambda x: x.lower())

    multicall.loc[multicall["to"] == address, "From Coin"] = gas_coin
    multicall.loc[multicall["to"] == address, "To Coin"] = multicall.loc[
        multicall["to"] == address, "tokenSymbol"]

    multicall.loc[multicall["from"] == address, "To Coin"] = gas_coin
    multicall.loc[multicall["from"] == address, "From Coin"] = multicall.loc[
        multicall["from"] == address, "tokenSymbol"]

    multicall.loc[multicall["to"] == address, "From Amount"] = -multicall.loc[
        multicall["to"] == address, "value_normal"]
    multicall.loc[multicall["to"] == address, "To Amount"] = multicall.loc[multicall["to"] == address, "value"]

    multicall.loc[multicall["from"] == address, "To Amount"] = multicall.loc[
        multicall["from"] == address, "value_normal"]
    multicall.loc[multicall["from"] == address, "From Amount"] = -multicall.loc[
        multicall["from"] == address, "value"]

    multicall["Fee"] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

    multicall["Tag"] = "Trade"
    multicall["Notes"] = "Sofi Swap - swapExactTokensForETH"

    sofi_out = pd.concat([sofi_out, multicall])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI SOFI SWAP SONO INCLUSE")

    sofi_out = sofi_out[[x for x in sofi_out.columns if x in columns_out]]
    sofi_out = sofi_out.sort_index()

    sofi_out = sofi_out.drop_duplicates()

    return sofi_out

def sync_swap(df, address, columns_out, gas_coin):
    sync_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    df = df[df["tokenSymbol"] != 'ySYNC']

    # Swap ETH
    swap_df = df[df["methodId"].isin(["0x2cc4081e"])].copy()
    df = pd.concat([swap_df, df]).drop_duplicates(keep=False)

    swap_df["value"] = eu.calculate_value_token(swap_df.value, swap_df.tokenDecimal)
    swap_df["value_normal"] = eu.calculate_value_eth(swap_df.value_normal)
    swap_df["value_internal"] = eu.calculate_value_eth(swap_df.value_internal)

    swap_df.loc[swap_df['from_internal'] == address, 'value_internal'] *= -1
    swap_df.loc[swap_df['from_normal'] == address, 'value_normal'] *= -1

    swap_df["value_normal"] += swap_df["value_internal"]
    swap_df["value_normal"] = swap_df["value_normal"].abs()

    swap_df["from"] = swap_df["from"].fillna('').apply(lambda x: x.lower())
    swap_df["to"] = swap_df["to"].fillna('').apply(lambda x: x.lower())

    count_df = swap_df.groupby(swap_df.index).agg({'hash': 'count'}).reset_index()
    if any(count_df['hash'] == 2):
        count_df.index = count_df['timeStamp_normal']
        count_df = count_df.drop('timeStamp_normal', axis=1)
        swap_df = pd.merge(count_df, swap_df, left_on='timeStamp_normal', right_index=True)
        for indexl in swap_df[swap_df['hash_x'] == 2].index.unique():
            if swap_df[swap_df.index == indexl]['value_internal'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value_internal'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value_internal'] = swap_df.loc[
                    swap_df.index == indexl, 'value_internal'].sum()

            if swap_df[swap_df.index == indexl]['value'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value'] = swap_df.loc[
                    swap_df.index == indexl, 'value'].sum()

    swap_df.loc[swap_df["to"] == address, "From Coin"] = gas_coin
    swap_df.loc[swap_df["to"] == address, "To Coin"] = swap_df.loc[swap_df["to"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["from"] == address, "To Coin"] = gas_coin
    swap_df.loc[swap_df["from"] == address, "From Coin"] = swap_df.loc[swap_df["from"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["to"] == address, "From Amount"] = -swap_df.loc[
        swap_df["to"] == address, "value_normal"]
    swap_df.loc[swap_df["to"] == address, "To Amount"] = swap_df.loc[swap_df["to"] == address, "value"]

    swap_df.loc[swap_df["from"] == address, "To Amount"] = swap_df.loc[
        swap_df["from"] == address, "value_normal"]
    swap_df.loc[swap_df["from"] == address, "From Amount"] = -swap_df.loc[
        swap_df["from"] == address, "value"]

    swap_df["Fee"] = eu.calculate_gas(swap_df.gasPrice, swap_df.gasUsed_normal)

    swap_df["Tag"] = "Trade"
    swap_df["Notes"] = "SyncSwap - swap eth"

    if any(count_df['hash'] == 2):
        swap_df = swap_df.drop_duplicates(subset=columns_out)
        swap_df = swap_df.drop('hash_x', axis=1)

    sync_out = pd.concat([sync_out, swap_df])

    # Swap tokens
    swap_df2 = df[df["methodId"].isin(["0xe84d494b"])].copy()
    df = pd.concat([swap_df2, df]).drop_duplicates(keep=False)

    swap_df2["value"] = eu.calculate_value_token(swap_df2.value, swap_df2.tokenDecimal)
    swap_df2["from"] = swap_df2["from"].apply(lambda x: x.lower())
    swap_df2["to"] = swap_df2["to"].apply(lambda x: x.lower())

    swap_df2['Fee'] /= 2
    swap_df2.loc[swap_df2['to'] != address, 'From Amount'] = -swap_df2.loc[
        swap_df2['to'] != address, 'value'].values
    swap_df2.loc[swap_df2['to'] == address, 'To Amount'] = swap_df2.loc[swap_df2['to'] == address, 'value'].values
    swap_df2.loc[swap_df2['to'] != address, 'From Coin'] = swap_df2.loc[
        swap_df2['to'] != address, 'tokenSymbol'].values
    swap_df2.loc[swap_df2['to'] == address, 'To Coin'] = swap_df2.loc[
        swap_df2['to'] == address, 'tokenSymbol'].values

    swap_df2[['From Coin', 'From Amount']] = swap_df2[['From Coin', 'From Amount']].ffill().infer_objects(
        copy=False)
    swap_df2[['To Coin', 'To Amount']] = swap_df2[['To Coin', 'To Amount']].bfill().infer_objects(copy=False)
    swap_df2 = swap_df2.drop_duplicates(subset=columns_out)
    swap_df2["Tag"] = "Trade"
    swap_df2["Notes"] = "SyncSwap - trade tokens"

    sync_out = pd.concat([sync_out, swap_df2])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["0x94ec6d78", "0x53c43f15", "0x7d10c9d6"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df.loc[liquidity_df['to'] == address, 'functionName'] = 'remove'
    liquidity_df.loc[liquidity_df['to'] != address, 'functionName'] = 'deposit'

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        liquidity_df = pd.concat([liquidity_df, temp_df]).drop_duplicates(keep=False)
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("deposit"),
            ["value_normal", "value"],
        ] = None
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "SyncSwap - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "SyncSwap - Withdraw"]

    liquidity_df = pd.concat([liquidity_df, liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")]])

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    liquidity_df = liquidity_df.sort_index()

    for indx in liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")].index.unique():
        if liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'From Amount'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Amount'].iloc[0]]
            liquidity_df.loc[liquidity_df.index == indx, 'From Coin'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Coin'].iloc[0]]
        if liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'To Amount'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0], None]
            liquidity_df.loc[liquidity_df.index == indx, 'To Coin'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Coin'].iloc[0], None]

    sync_out = pd.concat([sync_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI SYNSWAP SONO INCLUSE")

    sync_out = sync_out[[x for x in sync_out.columns if x in columns_out]]
    sync_out = sync_out.sort_index()

    sync_out = sync_out.drop_duplicates()

    return sync_out


def spacefi(df, address, columns_out, gas_coin):
    spacefi_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Swap ETH
    swap_df = df[df["methodId"].isin(["0xfb3bdb41"])].copy()
    df = pd.concat([swap_df, df]).drop_duplicates(keep=False)

    swap_df["value"] = eu.calculate_value_token(swap_df.value, swap_df.tokenDecimal)
    swap_df["value_normal"] = eu.calculate_value_eth(swap_df.value_normal)
    swap_df["value_internal"] = eu.calculate_value_eth(swap_df.value_internal)

    swap_df.loc[swap_df['from_internal'] == address, 'value_internal'] *= -1
    swap_df.loc[swap_df['from_normal'] == address, 'value_normal'] *= -1

    swap_df["value_normal"] += swap_df["value_internal"]
    swap_df["value_normal"] = swap_df["value_normal"].abs()

    swap_df["from"] = swap_df["from"].fillna('').apply(lambda x: x.lower())
    swap_df["to"] = swap_df["to"].fillna('').apply(lambda x: x.lower())

    count_df = swap_df.groupby(swap_df.index).agg({'hash': 'count'}).reset_index()
    if any(count_df['hash'] == 2):
        count_df.index = count_df['timeStamp_normal']
        count_df = count_df.drop('timeStamp_normal', axis=1)
        swap_df = pd.merge(count_df, swap_df, left_on='timeStamp_normal', right_index=True)
        for indexl in swap_df[swap_df['hash_x'] == 2].index.unique():
            if swap_df[swap_df.index == indexl]['value_internal'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value_internal'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value_internal'] = swap_df.loc[
                    swap_df.index == indexl, 'value_internal'].sum()

            if swap_df[swap_df.index == indexl]['value'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value'] = swap_df.loc[
                    swap_df.index == indexl, 'value'].sum()

    swap_df.loc[swap_df["to"] == address, "From Coin"] = gas_coin
    swap_df.loc[swap_df["to"] == address, "To Coin"] = swap_df.loc[swap_df["to"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["from"] == address, "To Coin"] = gas_coin
    swap_df.loc[swap_df["from"] == address, "From Coin"] = swap_df.loc[swap_df["from"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["to"] == address, "From Amount"] = -swap_df.loc[
        swap_df["to"] == address, "value_normal"]
    swap_df.loc[swap_df["to"] == address, "To Amount"] = swap_df.loc[swap_df["to"] == address, "value"]

    swap_df.loc[swap_df["from"] == address, "To Amount"] = swap_df.loc[
        swap_df["from"] == address, "value_normal"]
    swap_df.loc[swap_df["from"] == address, "From Amount"] = -swap_df.loc[
        swap_df["from"] == address, "value"]

    swap_df["Fee"] = eu.calculate_gas(swap_df.gasPrice, swap_df.gasUsed_normal)

    swap_df["Tag"] = "Trade"
    swap_df["Notes"] = "Spacefi - swap eth"

    spacefi_out = pd.concat([spacefi_out, swap_df])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["0xf305d719", "0x02751cec"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df.loc[liquidity_df['to'] == address, 'functionName'] = 'remove'
    liquidity_df.loc[liquidity_df['to'] != address, 'functionName'] = 'deposit'
    liquidity_df = liquidity_df[liquidity_df['tokenSymbol'] != 'SLP']

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        liquidity_df = pd.concat([liquidity_df, temp_df]).drop_duplicates(keep=False)
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("deposit"),
            ["value_normal", "value"],
        ] = None
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "SyncSwap - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "SyncSwap - Withdraw"]

    liquidity_df = pd.concat([liquidity_df, liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")]])

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    liquidity_df = liquidity_df.sort_index()

    for indx in liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")].index.unique():
        if liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'From Amount'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Amount'].iloc[0]]
            liquidity_df.loc[liquidity_df.index == indx, 'From Coin'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Coin'].iloc[0]]
        if liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'To Amount'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0], None]
            liquidity_df.loc[liquidity_df.index == indx, 'To Coin'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Coin'].iloc[0], None]

    spacefi_out = pd.concat([spacefi_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI SPACEFI SONO INCLUSE")

    spacefi_out = spacefi_out[[x for x in spacefi_out.columns if x in columns_out]]
    spacefi_out = spacefi_out.sort_index()

    spacefi_out = spacefi_out.drop_duplicates()

    return spacefi_out

def koi(df, address, columns_out, gas_coin):
    koi_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["0xaac57b19", "0x3a8e53ff"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df.loc[liquidity_df['to'] == address, 'functionName'] = 'remove'
    liquidity_df.loc[liquidity_df['to'] != address, 'functionName'] = 'deposit'

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        liquidity_df = pd.concat([liquidity_df, temp_df]).drop_duplicates(keep=False)
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("deposit"),
            ["value_normal", "value"],
        ] = None
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "SyncSwap - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "SyncSwap - Withdraw"]

    liquidity_df = pd.concat([liquidity_df, liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")]])

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    liquidity_df = liquidity_df.sort_index()

    for indx in liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")].index.unique():
        if liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'From Amount'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Amount'].iloc[0]]
            liquidity_df.loc[liquidity_df.index == indx, 'From Coin'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Coin'].iloc[0]]
        if liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'To Amount'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0], None]
            liquidity_df.loc[liquidity_df.index == indx, 'To Coin'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Coin'].iloc[0], None]

    koi_out = pd.concat([koi_out, liquidity_df])

    # Claim Fees
    claim_df = df[df["methodId"].isin(["0x"])].copy()
    df = pd.concat([claim_df, df]).drop_duplicates(keep=False)

    claim_df["value"] = eu.calculate_value_token(
        claim_df.value.fillna(0), claim_df.tokenDecimal.fillna(0)
    )

    claim_df['To Coin'] = claim_df['tokenSymbol']
    claim_df['To Amount'] = claim_df['value']
    claim_df[['Tag', 'Notes']] = ['Reward', 'Koi - withdraw']

    koi_out = pd.concat([koi_out, claim_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI KOI SONO INCLUSE")

    koi_out = koi_out[[x for x in koi_out.columns if x in columns_out]]
    koi_out = koi_out.sort_index()

    koi_out = koi_out.drop_duplicates()

    return koi_out


def era_lend(df, address, columns_out, gas_coin):
    era_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["0x", "0xdb006a75", "0x852a12e3"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df.loc[pd.isna(liquidity_df['to_internal']), 'functionName'] = 'deposit'
    liquidity_df.loc[~pd.isna(liquidity_df['to_internal']), 'functionName'] = 'remove'

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    liquidity_df["value_normal"] = liquidity_df["value_normal"].cumsum()
    liquidity_df.loc[liquidity_df["functionName"].str.contains("deposit"), ["value_normal", "value"]] = None

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "Era Lend - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "Era Lend - Withdraw"]

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    era_out = pd.concat([era_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI ERA LEND SONO INCLUSE")

    era_out = era_out[[x for x in era_out.columns if x in columns_out]]
    era_out = era_out.sort_index()

    era_out = era_out.drop_duplicates()

    return era_out


def on_finance(df, columns_out, gas_coin):
    on_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["0x376d3d5d", "0xfb1da1d6"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df.loc[pd.isna(liquidity_df['to_internal']), 'functionName'] = 'deposit'
    liquidity_df.loc[~pd.isna(liquidity_df['to_internal']), 'functionName'] = 'remove'

    osd_rewards = liquidity_df[np.logical_and(liquidity_df['functionName'] == 'remove',
                                              liquidity_df['tokenSymbol'] == 'OSD')].copy()
    liquidity_df = pd.concat([liquidity_df, osd_rewards]).drop_duplicates(keep=False)

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    liquidity_df["value_normal"] = liquidity_df["value_normal"].cumsum()
    liquidity_df.loc[liquidity_df["functionName"].str.contains("deposit"), ["value_normal", "value"]] = None

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "ON Finance - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "ON Finance - Withdraw"]

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    if osd_rewards.shape[0] > 0:
        osd_rewards['To Coin'] = 'OSD'
        osd_rewards['To Amount'] = eu.calculate_value_token(osd_rewards['value'], osd_rewards['tokenDecimal'])
        osd_rewards['Fee'] = None
        osd_rewards[["Tag", "Notes"]] = ["Reward", "ON Finance- Withdraw"]
        liquidity_df = pd.concat([liquidity_df, osd_rewards])

    on_out = pd.concat([on_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI ON CHAIN FINANCE SONO INCLUSE")

    on_out = on_out[[x for x in on_out.columns if x in columns_out]]
    on_out = on_out.sort_index()

    on_out = on_out.drop_duplicates()

    return on_out


def izumi(df, address, columns_out, gas_coin):
    izumi_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    df.loc[
        df['to_erc721'].fillna('').apply(lambda x: x.lower()) == address, ['methodId', 'functionName']] = [
        'liquidity', 'deposit']

    # Remove liq
    df.loc[np.logical_and(df['to_internal'] == df['to'],
                          pd.isna(df['to_erc721'])), ['methodId', 'functionName']] = ['liquidity', 'remove']

    # Swap ETH
    swap_df = df[df["methodId"].isin(["0xac9650d8"])].copy()
    df = pd.concat([swap_df, df]).drop_duplicates(keep=False)

    swap_df["value"] = eu.calculate_value_token(swap_df.value, swap_df.tokenDecimal)
    swap_df["value_normal"] = eu.calculate_value_eth(swap_df.value_normal)
    swap_df["value_internal"] = eu.calculate_value_eth(swap_df.value_internal)

    swap_df.loc[swap_df['from_internal'] == address, 'value_internal'] *= -1
    swap_df.loc[swap_df['from_normal'] == address, 'value_normal'] *= -1

    swap_df["value_normal"] += swap_df["value_internal"]
    swap_df["value_normal"] = swap_df["value_normal"].abs()

    swap_df["from"] = swap_df["from"].fillna('').apply(lambda x: x.lower())
    swap_df["to"] = swap_df["to"].fillna('').apply(lambda x: x.lower())

    count_df = swap_df.groupby(swap_df.index).agg({'hash': 'count'}).reset_index()
    if any(count_df['hash'] == 2):
        count_df.index = count_df['timeStamp_normal']
        count_df = count_df.drop('timeStamp_normal', axis=1)
        swap_df = pd.merge(count_df, swap_df, left_on='timeStamp_normal', right_index=True)
        for indexl in swap_df[swap_df['hash_x'] == 2].index.unique():
            if swap_df[swap_df.index == indexl]['value_internal'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value_internal'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value_internal'] = swap_df.loc[
                    swap_df.index == indexl, 'value_internal'].sum()

            if swap_df[swap_df.index == indexl]['value'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value'] = swap_df.loc[
                    swap_df.index == indexl, 'value'].sum()

    swap_df.loc[swap_df["to"] == address, "From Coin"] = gas_coin
    swap_df.loc[swap_df["to"] == address, "To Coin"] = swap_df.loc[swap_df["to"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["from"] == address, "To Coin"] = gas_coin
    swap_df.loc[swap_df["from"] == address, "From Coin"] = swap_df.loc[swap_df["from"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["to"] == address, "From Amount"] = -swap_df.loc[
        swap_df["to"] == address, "value_normal"]
    swap_df.loc[swap_df["to"] == address, "To Amount"] = swap_df.loc[swap_df["to"] == address, "value"]

    swap_df.loc[swap_df["from"] == address, "To Amount"] = swap_df.loc[
        swap_df["from"] == address, "value_normal"]
    swap_df.loc[swap_df["from"] == address, "From Amount"] = -swap_df.loc[
        swap_df["from"] == address, "value"]

    swap_df["Fee"] = eu.calculate_gas(swap_df.gasPrice, swap_df.gasUsed_normal)

    swap_df["Tag"] = "Trade"
    swap_df["Notes"] = "iZUMi - swap eth"

    if any(count_df['hash'] == 2):
        swap_df = swap_df.drop_duplicates(subset=columns_out)
        swap_df = swap_df.drop('hash_x', axis=1)

    izumi_out = pd.concat([izumi_out, swap_df])

    # Burning position NFT
    burn_df = df[df["methodId"].isin(["0x42966c68"])].copy()
    df = pd.concat([burn_df, df]).drop_duplicates(keep=False)

    burn_df["Tag"] = "Trade"
    burn_df["Notes"] = "iZUMi - burn nft"

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["liquidity"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        liquidity_df = pd.concat([liquidity_df, temp_df]).drop_duplicates(keep=False)
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("deposit"),
            ["value_normal", "value"],
        ] = None
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "iZUMi - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "iZUMi - Withdraw"]

    liquidity_df = pd.concat([liquidity_df, liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")]])

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    liquidity_df = liquidity_df.sort_index()

    for indx in liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")].index.unique():
        if liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'From Amount'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Amount'].iloc[0]]
            liquidity_df.loc[liquidity_df.index == indx, 'From Coin'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Coin'].iloc[0]]
        if liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'To Amount'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0], None]
            liquidity_df.loc[liquidity_df.index == indx, 'To Coin'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Coin'].iloc[0], None]

    izumi_out = pd.concat([izumi_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI IZUMI SONO INCLUSE")

    izumi_out = izumi_out[[x for x in izumi_out.columns if x in columns_out]]
    izumi_out = izumi_out.sort_index()

    izumi_out = izumi_out.drop_duplicates()

    return izumi_out


def deri_finance(df, address, columns_out, gas_coin):
    deri_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["0x4355bcd6", "0x489d6b06", "0x1e83409a"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df.loc[liquidity_df['to'] == address, 'functionName'] = 'remove'
    liquidity_df.loc[liquidity_df['to'] != address, 'functionName'] = 'deposit'

    deri_rewards = liquidity_df[np.logical_and(liquidity_df['functionName'] == 'remove',
                                               liquidity_df['tokenSymbol'] == 'DERI')].copy()
    liquidity_df = pd.concat([liquidity_df, deri_rewards]).drop_duplicates(keep=False)

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()

    liquidity_df["value_normal"] = liquidity_df["value_normal"].cumsum()
    liquidity_df.loc[liquidity_df["functionName"].str.contains("deposit"), ["value_normal", "value"]] = None

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "Deri Finance - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "Deri Finance - Withdraw"]

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    if deri_rewards.shape[0] > 0:
        deri_rewards['To Coin'] = 'Deri'
        deri_rewards['To Amount'] = eu.calculate_value_token(deri_rewards['value'], deri_rewards['tokenDecimal'])
        deri_rewards[["Tag", "Notes"]] = ["Reward", "Deri Finance- Withdraw"]
        liquidity_df = pd.concat([liquidity_df, deri_rewards])

    deri_out = pd.concat([deri_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI DERI SONO INCLUSE")

    deri_out = deri_out[[x for x in deri_out.columns if x in columns_out]]
    deri_out = deri_out.sort_index()

    deri_out = deri_out.drop_duplicates()

    return deri_out


def maverick(df, address, columns_out, gas_coin):
    maverick_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    df.loc[df['to_erc721'].fillna('').apply(lambda x: x.lower()) == address, ['methodId', 'functionName']] = ['liquidity', 'deposit']
    df.loc[df['to_internal']!=df['from_normal'], ['methodId', 'functionName']] = ['liquidity', 'deposit']

    df.loc[df['to']=='', ['methodId', 'functionName']] = ['liquidity', 'remove']

    # Swap ETH
    swap_df = df[df["methodId"].isin(["0xac9650d8"])].copy()
    df = pd.concat([swap_df, df]).drop_duplicates(keep=False)

    swap_df["value"] = eu.calculate_value_token(swap_df.value, swap_df.tokenDecimal)
    swap_df["value_normal"] = eu.calculate_value_eth(swap_df.value_normal)
    swap_df["value_internal"] = eu.calculate_value_eth(swap_df.value_internal)

    swap_df.loc[swap_df['from_internal'] == address, 'value_internal'] *= -1
    swap_df.loc[swap_df['from_normal'] == address, 'value_normal'] *= -1

    swap_df["value_normal"] += swap_df["value_internal"]
    swap_df["value_normal"] = swap_df["value_normal"].abs()

    swap_df["from"] = swap_df["from"].fillna('').apply(lambda x: x.lower())
    swap_df["to"] = swap_df["to"].fillna('').apply(lambda x: x.lower())

    count_df = swap_df.groupby(swap_df.index).agg({'hash': 'count'}).reset_index()
    if any(count_df['hash'] == 2):
        count_df.index = count_df['timeStamp_normal']
        count_df = count_df.drop('timeStamp_normal', axis=1)
        swap_df = pd.merge(count_df, swap_df, left_on='timeStamp_normal', right_index=True)
        for indexl in swap_df[swap_df['hash_x'] == 2].index.unique():
            if swap_df[swap_df.index == indexl]['value_internal'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value_internal'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value_internal'] = swap_df.loc[
                    swap_df.index == indexl, 'value_internal'].sum()

            if swap_df[swap_df.index == indexl]['value'].iloc[0] != \
                    swap_df[swap_df.index == indexl]['value'].iloc[1]:
                swap_df.loc[swap_df.index == indexl, 'value'] = swap_df.loc[
                    swap_df.index == indexl, 'value'].sum()

    swap_df.loc[swap_df["to"] == address, "From Coin"] = gas_coin
    swap_df.loc[swap_df["to"] == address, "To Coin"] = swap_df.loc[swap_df["to"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["from"] == address, "To Coin"] = gas_coin
    swap_df.loc[swap_df["from"] == address, "From Coin"] = swap_df.loc[swap_df["from"] == address, "tokenSymbol"]

    swap_df.loc[swap_df["to"] == address, "From Amount"] = -swap_df.loc[
        swap_df["to"] == address, "value_normal"]
    swap_df.loc[swap_df["to"] == address, "To Amount"] = swap_df.loc[swap_df["to"] == address, "value"]

    swap_df.loc[swap_df["from"] == address, "To Amount"] = swap_df.loc[
        swap_df["from"] == address, "value_normal"]
    swap_df.loc[swap_df["from"] == address, "From Amount"] = -swap_df.loc[
        swap_df["from"] == address, "value"]

    swap_df["Fee"] = eu.calculate_gas(swap_df.gasPrice, swap_df.gasUsed_normal)

    swap_df["Tag"] = "Trade"
    swap_df["Notes"] = "iZUMi - swap eth"

    if any(count_df['hash'] == 2):
        swap_df = swap_df.drop_duplicates(subset=columns_out)
        swap_df = swap_df.drop('hash_x', axis=1)

    maverick_out = pd.concat([maverick_out, swap_df])

    # Adding and removing liquidity with ETH
    liquidity_df = df[df["methodId"].isin(["liquidity"])].copy()
    df = pd.concat([liquidity_df, df]).drop_duplicates(keep=False)

    liquidity_df["value"] = eu.calculate_value_token(
        liquidity_df.value.fillna(0), liquidity_df.tokenDecimal.fillna(0)
    )
    liquidity_df["value_normal"] = eu.calculate_value_eth(
        liquidity_df.value_normal.fillna(0)
    )
    liquidity_df["value_internal"] = eu.calculate_value_eth(
        liquidity_df.value_internal.fillna(0)
    )
    liquidity_df["value_normal"] += liquidity_df["value_internal"]

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["value_normal", "value"],
    ] *= -1
    liquidity_df = liquidity_df.sort_index()
    liquidity_df['tokenSymbol'] = liquidity_df['tokenSymbol'].ffill()

    for token in liquidity_df["tokenSymbol"].unique():
        temp_df = liquidity_df[liquidity_df["tokenSymbol"] == token].copy()
        liquidity_df = pd.concat([liquidity_df, temp_df]).drop_duplicates(keep=False)
        temp_df["value"] = temp_df["value"].cumsum()
        temp_df["value_normal"] = temp_df["value_normal"].cumsum()
        temp_df.loc[
            temp_df["functionName"].str.contains("deposit"),
            ["value_normal", "value"],
        ] = None
        liquidity_df = pd.concat([liquidity_df, temp_df])

    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("deposit"),
        ["Tag", "Notes"],
    ] = ["Movement", "Maverick - Deposit"]
    liquidity_df.loc[
        liquidity_df["functionName"].str.contains("remove"),
        ["Tag", "Notes"],
    ] = ["Reward", "Maverick - Withdraw"]

    liquidity_df = pd.concat([liquidity_df, liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")]])
    liquidity_df.loc[liquidity_df["functionName"].str.contains("remove"), 'Fee'] /= 2

    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] < 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Amount"
    ] = liquidity_df.loc[liquidity_df["value_normal"] > 0, "value_normal"]
    liquidity_df.loc[liquidity_df["value_normal"] < 0, "From Coin"] = gas_coin
    liquidity_df.loc[liquidity_df["value_normal"] > 0, "To Coin"] = gas_coin

    liquidity_df.loc[liquidity_df["value"] < 0, "From Amount"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Amount"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "value"
    ]
    liquidity_df.loc[liquidity_df["value"] < 0, "From Coin"] = liquidity_df.loc[
        liquidity_df["value"] < 0, "tokenSymbol"
    ]
    liquidity_df.loc[liquidity_df["value"] > 0, "To Coin"] = liquidity_df.loc[
        liquidity_df["value"] > 0, "tokenSymbol"
    ]

    liquidity_df = liquidity_df.sort_index()

    for indx in liquidity_df.loc[liquidity_df["functionName"].str.contains("remove")].index.unique():
        if liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'From Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'From Amount'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Amount'].iloc[0]]
            liquidity_df.loc[liquidity_df.index == indx, 'From Coin'] = [None, liquidity_df.loc[
                liquidity_df.index == indx, 'From Coin'].iloc[0]]
        if liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0] == \
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[1]:
            liquidity_df.loc[liquidity_df.index == indx, 'To Amount'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Amount'].iloc[0], None]
            liquidity_df.loc[liquidity_df.index == indx, 'To Coin'] = [
                liquidity_df.loc[liquidity_df.index == indx, 'To Coin'].iloc[0], None]

    maverick_out = pd.concat([maverick_out, liquidity_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI IZUMI SONO INCLUSE")

    maverick_out = maverick_out[[x for x in maverick_out.columns if x in columns_out]]
    maverick_out = maverick_out.sort_index()

    maverick_out = maverick_out.drop_duplicates()

    return maverick_out