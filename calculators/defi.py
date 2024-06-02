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

        multicall.loc[multicall["from_internal"] == address, "From Coin"] = "ETH"
        multicall.loc[multicall["from_internal"] == address, "To Coin"] = multicall.loc[
            multicall["from_internal"] == address, "tokenSymbol"
        ]

        multicall.loc[multicall["to_internal"] == address, "To Coin"] = "ETH"
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

        multicall.loc[multicall["from_internal"] == address, "From Coin"] = "ETH"
        multicall.loc[multicall["from_internal"] == address, "To Coin"] = multicall.loc[
            multicall["from_internal"] == address, "tokenSymbol"
        ]

        multicall.loc[multicall["to_internal"] == address, "To Coin"] = "ETH"
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
        if temp_df.shape[0] > 0:
            print("swapETH FOUND!!")
        temp_df["value_normal"] = [-int(x) / 10**18 for x in temp_df["value_normal"]]
        temp_df = temp_df[
            [
                "timeStamp_normal",
                "from_normal",
                "to_normal",
                "gasUsed_normal",
                "gasPrice",
                "value_normal",
            ]
        ]
        temp_df.columns = [
            "Timestamp",
            "From",
            "To",
            "Gasused",
            "Gasprice",
            "From Amount",
        ]
        temp_df["From Coin"] = gas_coin
        temp_df["Kind"] = "Stargate - Bridge ETH"

        temp_df["Gasused"] = [int(x) for x in temp_df["Gasused"]]
        for i in set(temp_df["Timestamp"]):
            temp_df.loc[temp_df["Timestamp"] == i, "Gasused"] /= temp_df[
                temp_df["Timestamp"] == i
            ].shape[0]

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
