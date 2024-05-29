import pandas as pd
import requests
import calculators.evm_utils as eu


def uniswap(df, address, columns_out):
    uniswap_out = pd.DataFrame()

    df.index = df['timeStamp_normal']

    if df.shape[0] > 0:
        # Function multicall
        multicall = df[df['functionName'].str.contains('multicall')].copy()
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

        multicall.loc[multicall['from_internal'] == address, 'From Amount'] = multicall.loc[
            multicall['from_internal'] == address, 'value_normal']
        multicall.loc[multicall['from_internal'] == address, 'To Amount'] = multicall.loc[
            multicall['from_internal'] == address, 'value']

        multicall.loc[multicall['to_internal'] == address, 'To Amount'] = multicall.loc[
            multicall['to_internal'] == address, 'value_normal']
        multicall.loc[multicall['to_internal'] == address, 'From Amount'] = -multicall.loc[
            multicall['to_internal'] == address, 'value']

        multicall['Fee'] = eu.calculate_gas(multicall.gasPrice, multicall.gasUsed_normal)

        uniswap_out = pd.concat([uniswap_out, multicall])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI UNISWAP SONO INCLUSE")

    uniswap_out = uniswap_out[[x for x in uniswap_out.columns if x in columns_out]]
    uniswap_out = uniswap_out.sort_index()

    uniswap_out['Tag'] = 'Uniswap'
    uniswap_out['Notes'] = 'Swap'

    return uniswap_out
