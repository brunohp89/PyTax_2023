import pandas as pd
import requests
import calculators.evm_utils as eu
import numpy as np


def uniswap(df, address, columns_out, gas_coin):
    uniswap_out = pd.DataFrame()

    df.index = df["timeStamp_normal"]

    if df.shape[0] > 0:
        # Function multicall V2
        multicall = df[
            np.logical_and(
                df["functionName"].str.contains("multicall"),
                df["to_normal"] == "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",
            )
        ].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall["value"] = eu.calculate_value_token(
            multicall.value, multicall.tokenDecimal
        )
        multicall["value_normal"] = eu.calculate_value_eth(multicall.value_normal)
        multicall["value_internal"] = eu.calculate_value_eth(multicall.value_internal)
        multicall["value_normal"] += multicall["value_internal"]

        multicall["from_internal"] = multicall["from_internal"].apply(
            lambda x: x.lower()
        )
        multicall["to_internal"] = multicall["to_internal"].apply(lambda x: x.lower())

        multicall["from"] = multicall["from"].apply(lambda x: x.lower())
        multicall["to"] = multicall["to"].apply(lambda x: x.lower())

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

        multicall["Tag"] = "Uniswap"
        multicall["Notes"] = "Swap"

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
                df["functionName"].str.contains("execute"),
                df["functionName"].str.contains("swapExactTokensForETH".lower()),
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

        multicall["Tag"] = "Uniswap"
        multicall["Notes"] = "Swap"

        uniswap_out = pd.concat([uniswap_out, multicall])
    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI UNISWAP SONO INCLUSE")

    uniswap_out = uniswap_out[[x for x in uniswap_out.columns if x in columns_out]]
    uniswap_out = uniswap_out.sort_index()

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
