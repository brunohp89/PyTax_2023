import pandas as pd
import requests
import calculators.evm_utils as eu
import numpy as np


def uniswap(df, address, columns_out, gas_coin):
    uniswap_out = pd.DataFrame()

    df.index = df['timeStamp_normal']

    if df.shape[0] > 0:
        # Function multicall V2
        multicall = df[np.logical_and(df['functionName'].str.contains('multicall'),
                                      df['to_normal'] == '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45')].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall['value'] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)
        multicall['value_normal'] = eu.calculate_value_eth(multicall.value_normal)
        multicall['value_internal'] = eu.calculate_value_eth(multicall.value_internal)
        multicall['value_normal'] += multicall['value_internal']

        multicall['from_internal'] = multicall['from_internal'].apply(lambda x: x.lower())
        multicall['to_internal'] = multicall['to_internal'].apply(lambda x: x.lower())

        multicall['from'] = multicall['from'].apply(lambda x: x.lower())
        multicall['to'] = multicall['to'].apply(lambda x: x.lower())

        multicall.loc[multicall['from_internal'] == address, 'From Coin'] = 'ETH'
        multicall.loc[multicall['from_internal'] == address, 'To Coin'] = multicall.loc[
            multicall['from_internal'] == address, 'tokenSymbol']

        multicall.loc[multicall['to_internal'] == address, 'To Coin'] = 'ETH'
        multicall.loc[multicall['to_internal'] == address, 'From Coin'] = multicall.loc[
            multicall['to_internal'] == address, 'tokenSymbol']

        multicall.loc[multicall['from_internal'] == address, 'From Amount'] = -multicall.loc[
            multicall['from_internal'] == address, 'value_normal']
        multicall.loc[multicall['from_internal'] == address, 'To Amount'] = multicall.loc[
            multicall['from_internal'] == address, 'value']

        multicall.loc[multicall['to_internal'] == address, 'To Amount'] = multicall.loc[
            multicall['to_internal'] == address, 'value_normal']
        multicall.loc[multicall['to_internal'] == address, 'From Amount'] = -multicall.loc[
            multicall['to_internal'] == address, 'value']

        multicall['Fee'] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

        multicall['Tag'] = 'Uniswap'
        multicall['Notes'] = 'Swap'

        uniswap_out = pd.concat([uniswap_out, multicall])

        # Multicall V3
        multicall = df[np.logical_and(df['functionName'].str.contains('multicall'),
                                      df['to_normal'] == '0xc36442b4a4522e871399cd717abdd847ab11fe88')].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall['Fee'] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)
        multicall.loc[pd.isna(multicall['tokenSymbol_erc721']), ['Tag', 'Notes']] = ['Movement', 'Uniswap-V3 Deposit']
        multicall.loc[~pd.isna(multicall['tokenSymbol_erc721']), ['Tag', 'Notes']] = ['Movement', 'Uniswap-V3 Withdraw']

        uniswap_out = pd.concat([uniswap_out, multicall])

        # Adding and removing liquidity with ETH
        liquidity_df = df[np.logical_or(df['functionName'].str.contains('addliquidityeth'),
                                        df['functionName'].str.contains('removeliquidity'))].copy()
        df = pd.concat([df, liquidity_df]).drop_duplicates(keep=False)

        liquidity_df = liquidity_df[liquidity_df['tokenSymbol'] != 'UNI-V2']

        liquidity_df['Fee'] = eu.calculate_gas(liquidity_df.gasPrice, liquidity_df.gasUsed_normal)

        liquidity_df['value'] = eu.calculate_value_token(liquidity_df.value, liquidity_df.tokenDecimal)
        liquidity_df['value_normal'] = eu.calculate_value_eth(liquidity_df.value_normal.fillna(0))
        liquidity_df['value_internal'] = eu.calculate_value_eth(liquidity_df.value_internal.fillna(0))
        liquidity_df['value_normal'] += liquidity_df['value_internal']

        liquidity_df.loc[pd.isna(liquidity_df['to_internal']), 'to_internal'] = liquidity_df.loc[
            pd.isna(liquidity_df['to_internal']), 'to_normal']

        liquidity_df.loc[liquidity_df['functionName'].str.contains('addliquidityeth'), ['value_normal', 'value']] *= -1
        liquidity_df = liquidity_df.sort_index()

        for token in liquidity_df['tokenSymbol'].unique():
            temp_df = liquidity_df[liquidity_df['tokenSymbol'] == token]
            temp_df['value'] = temp_df['value'].cumsum()
            temp_df['value_normal'] = temp_df['value_normal'].cumsum()
            temp_df.loc[temp_df['functionName'].str.contains('addliquidityeth'), ['value_normal', 'value']] = None
            temp_df = pd.concat([temp_df, temp_df[temp_df['functionName'].str.contains('removeliquidity')]])
            temp_df.loc[temp_df['functionName'].str.contains('removeliquidity'), 'value'] *= [1, 0]
            temp_df.loc[temp_df['functionName'].str.contains('removeliquidity'), 'value_normal'] *= [0, 1]
            temp_df.loc[temp_df['functionName'].str.contains('removeliquidity'), 'Fee'] /= len(
                temp_df.loc[temp_df['functionName'].str.contains('removeliquidity'), 'Fee'])
            liquidity_df = liquidity_df.drop(temp_df.index, axis=0)
            liquidity_df = pd.concat([liquidity_df, temp_df])

        liquidity_df.loc[liquidity_df['functionName'].str.contains('addliquidityeth'), ['Tag', 'Notes']] = ['Movement',
                                                                                                            'Uniswap-V2 Deposit']
        liquidity_df.loc[liquidity_df['functionName'].str.contains('removeliquidity'), ['Tag', 'Notes']] = ['Reward',
                                                                                                            'Uniswap-V2 Withdraw']

        liquidity_df.loc[liquidity_df['value_normal'] < 0, 'From Amount'] = liquidity_df.loc[
            liquidity_df['value_normal'] < 0, 'value_normal']
        liquidity_df.loc[liquidity_df['value_normal'] > 0, 'To Amount'] = liquidity_df.loc[
            liquidity_df['value_normal'] > 0, 'value_normal']
        liquidity_df.loc[liquidity_df['value_normal'] < 0, 'From Coin'] = gas_coin
        liquidity_df.loc[liquidity_df['value_normal'] > 0, 'To Coin'] = gas_coin

        liquidity_df.loc[liquidity_df['value'] < 0, 'From Amount'] = liquidity_df.loc[
            liquidity_df['value'] < 0, 'value']
        liquidity_df.loc[liquidity_df['value'] > 0, 'To Amount'] = liquidity_df.loc[liquidity_df['value'] > 0, 'value']
        liquidity_df.loc[liquidity_df['value'] < 0, 'From Coin'] = liquidity_df.loc[
            liquidity_df['value'] < 0, 'tokenSymbol']
        liquidity_df.loc[liquidity_df['value'] > 0, 'To Coin'] = liquidity_df.loc[
            liquidity_df['value'] > 0, 'tokenSymbol']

        uniswap_out = pd.concat([uniswap_out, liquidity_df])

        # Function EXECUTE
        multicall = df[df['functionName'].str.contains('execute')].copy()
        df = pd.concat([df, multicall]).drop_duplicates(keep=False)

        multicall['value'] = eu.calculate_value_token(multicall.value, multicall.tokenDecimal)
        multicall['value_normal'] = eu.calculate_value_eth(multicall.value_normal.fillna(0))
        multicall['value_internal'] = eu.calculate_value_eth(multicall.value_internal.fillna(0))
        multicall['value_normal'] += multicall['value_internal']

        multicall['from_internal'] = multicall['from_internal'].combine_first(multicall['from_normal']).apply(
            lambda x: x.lower())
        multicall['to_internal'] = multicall['to_internal'].combine_first(multicall['to_normal']).apply(
            lambda x: x.lower())

        multicall['from'] = multicall['from'].apply(lambda x: x.lower())
        multicall['to'] = multicall['to'].apply(lambda x: x.lower())

        multicall.loc[multicall['from_internal'] == address, 'From Coin'] = 'ETH'
        multicall.loc[multicall['from_internal'] == address, 'To Coin'] = multicall.loc[
            multicall['from_internal'] == address, 'tokenSymbol']

        multicall.loc[multicall['to_internal'] == address, 'To Coin'] = 'ETH'
        multicall.loc[multicall['to_internal'] == address, 'From Coin'] = multicall.loc[
            multicall['to_internal'] == address, 'tokenSymbol']

        multicall.loc[multicall['from_internal'] == address, 'From Amount'] = -multicall.loc[
            multicall['from_internal'] == address, 'value_normal']
        multicall.loc[multicall['from_internal'] == address, 'To Amount'] = multicall.loc[
            multicall['from_internal'] == address, 'value']

        multicall.loc[multicall['to_internal'] == address, 'To Amount'] = multicall.loc[
            multicall['to_internal'] == address, 'value_normal']
        multicall.loc[multicall['to_internal'] == address, 'From Amount'] = -multicall.loc[
            multicall['to_internal'] == address, 'value']

        multicall['Fee'] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

        multicall['Tag'] = 'Uniswap'
        multicall['Notes'] = 'Swap'

        uniswap_out = pd.concat([uniswap_out, multicall])
    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI UNISWAP SONO INCLUSE")

    uniswap_out = uniswap_out[[x for x in uniswap_out.columns if x in columns_out]]
    uniswap_out = uniswap_out.sort_index()

    return uniswap_out


def love(df, columns_out):
    df.index = df['timeStamp_normal']
    df['value'] = eu.calculate_value_token(df['value'], df['tokenDecimal'])
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])

    # Claim
    df.loc[df['functionName'].str.contains('claim'), 'To Coin'] = 'LOVE'
    df.loc[df['functionName'].str.contains('claim'), 'To Amount'] = df.loc[
        df['functionName'].str.contains('claim'), 'value']
    df.loc[df['functionName'].str.contains('claim'), ['Tag', 'Notes']] = ['Reward', 'LOVE Airdrop']

    # Deposit or withdraw from smart contract
    love_dep_with = df[np.logical_or(df['functionName'].str.contains('deposit'),
                                     df['functionName'].str.contains('withdraw'))]
    love_dep_with = love_dep_with[love_dep_with['tokenSymbol'] != 'UNI-V2'].sort_index()
    love_dep_with['value'] = love_dep_with['value'].cumsum()
    love_dep_with.loc[love_dep_with['functionName'].str.contains('deposit'), ['To Coin', 'Tag', 'Notes']] = ['LOVE',
                                                                                                             'Movement',
                                                                                                             'Deposit LOVE']
    love_dep_with.loc[love_dep_with['Notes'] == 'Deposit LOVE', 'To Amount'] = None
    love_dep_with.loc[love_dep_with['functionName'].str.contains('withdraw'), ['To Coin', 'Tag', 'Notes']] = ['LOVE',
                                                                                                              'Reward',
                                                                                                              'Withdraw LOVE']
    love_dep_with['To Amount'] = love_dep_with['value']

    df = pd.concat([df[df['functionName'].str.contains('claim')], love_dep_with])
    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    return df
