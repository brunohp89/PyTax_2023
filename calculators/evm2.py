import requests
import pandas as pd
import numpy as np
from PricesClass import Prices
import tax_library as tx
import os
from utils import date_from_timestamp
import calculators.evm_utils as eu
import calculators.nft_utils as nu
import calculators.defi as defi

address, chain, scan_key = (
    '0xd6ED446e05af56956D4193c018f6D1b6D000BF2b', 'eth-mainnet', 'GFID2HN2QCS6UR4K1CX13F946P2V1S7Q7X')


def get_transactions_df(address, chain, scan_key=None):
    address = address.lower()
    columns_out = ['From', 'To', 'From Coin', 'To Coin', 'From Amount', 'To Amount', 'Fee', 'Fee Coin', 'Fee Fiat',
                   'Fiat', 'Fiat Price', 'Tag', 'Source', 'Notes']

    # Getting all transactions
    trx_df = eu.get_transactions_raw(address, chain, scan_key)
    trx_df[1][columns_out] = None

    gas_coin = trx_df[0]
    trx_df = trx_df[1]

    trx_df[['value_normal', 'gas_normal', 'gasUsed_normal', 'gasPrice']] = trx_df[
        ['value_normal', 'gas_normal', 'gasUsed_normal', 'gasPrice']].fillna(0)

    trx_df_raw = trx_df.copy()

    vout = pd.DataFrame()

    # ------------------------------------------------------------------------------------------------------------------
    # Normal ETH transfers ---------------------------------------------------------------------------------------------
    eth_transfers_df = eu.eth_transfers(trx_df[trx_df['input_normal'] == '0x'].copy(), address, gas_coin, columns_out)
    trx_df = trx_df[trx_df['input_normal'] != '0x']

    vout = pd.concat([vout, eth_transfers_df])
    # END Normal ETH transfers -----------------------------------------------------------------------------------------
    del eth_transfers_df
    # Normal ERC20 transfers -------------------------------------------------------------------------------------------
    erc20_transfers_df = trx_df[~pd.isna(trx_df['tokenSymbol'])].copy()
    erc20_transfers_df = erc20_transfers_df[
        np.logical_or(pd.isna(erc20_transfers_df['blockNumber_normal']), erc20_transfers_df['value_normal'] == '0')]
    erc20_transfers_df = erc20_transfers_df[pd.isna(erc20_transfers_df['value_internal'])]
    erc20_transfers_df = erc20_transfers_df[np.logical_and(pd.isna(erc20_transfers_df['blockNumber_erc721']),
                                                           pd.isna(erc20_transfers_df['blockNumber_erc1155']))]

    trx_df = pd.concat([erc20_transfers_df, trx_df]).drop_duplicates(keep=False)

    erc20_transfers_df = eu.erc20_transfer(erc20_transfers_df, address, columns_out)
    vout = pd.concat([vout, erc20_transfers_df])
    # END Normal ERC20 transfers ---------------------------------------------------------------------------------------
    del erc20_transfers_df
    # Normal ERC721 transfers ------------------------------------------------------------------------------------------
    erc721_transfers_df = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber_erc721']), pd.isna(trx_df['blockNumber_erc1155']))].copy()
    erc721_transfers_df = erc721_transfers_df[
        np.logical_and(pd.isna(erc721_transfers_df['blockNumber_normal']), pd.isna(erc721_transfers_df['blockNumber']))]

    trx_df = pd.concat([erc721_transfers_df, trx_df]).drop_duplicates(keep=False)

    erc721_transfers_df = eu.erc721_transfer(erc721_transfers_df, address, columns_out)
    vout = pd.concat([vout, erc721_transfers_df])
    # END Normal ERC721 transfers --------------------------------------------------------------------------------------
    del erc721_transfers_df
    # Normal ERC1155 transfers -----------------------------------------------------------------------------------------
    erc1155_transfers_df = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber_erc1155']), pd.isna(trx_df['blockNumber_erc721']))].copy()
    erc1155_transfers_df = erc1155_transfers_df[
        np.logical_and(pd.isna(erc1155_transfers_df['blockNumber_normal']),
                       pd.isna(erc1155_transfers_df['blockNumber']))]

    trx_df = pd.concat([erc1155_transfers_df, trx_df]).drop_duplicates(keep=False)

    erc1155_transfers_df = eu.erc1155_transfer(erc1155_transfers_df, address, columns_out)
    vout = pd.concat([vout, erc1155_transfers_df])
    # END Normal ER1155 transfers --------------------------------------------------------------------------------------
    del erc1155_transfers_df
    # Approvals --------------------------------------------------------------------------------------------------------
    trx_df['functionName'] = trx_df['functionName'].apply(lambda x: str(x).lower())

    approvals_df = trx_df[np.logical_and(trx_df['functionName'].str.contains('approv'), pd.isna(trx_df['blockNumber']))]
    approvals_df.index = approvals_df['timeStamp_normal']
    approvals_df = approvals_df[
        np.logical_and(pd.isna(approvals_df['blockNumber_internal']), pd.isna(approvals_df['blockNumber_erc721']))]
    approvals_df = approvals_df[pd.isna(approvals_df['blockNumber_erc1155'])]

    trx_df = pd.concat([approvals_df, trx_df]).drop_duplicates(keep=False)

    approvals_df[['To', 'From']] = approvals_df[['to_normal', 'from_normal']]
    approvals_df['Fee'] = eu.calculate_gas(approvals_df.gasPrice, approvals_df.gasUsed_normal)
    approvals_df = approvals_df[[x for x in approvals_df.columns if x in columns_out]]
    approvals_df = approvals_df.sort_index()

    approvals_df['Tag'] = 'Set Approval'

    vout = pd.concat([approvals_df, vout]).drop_duplicates(keep=False)
    # END Approvals ----------------------------------------------------------------------------------------------------
    del approvals_df
    # WETH -------------------------------------------------------------------------------------------------------------

    # WETH wrapping
    weth_contracts = [
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(),
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
        "0x5c7f8a570d578ed84e63fdfa7b1ee72deae1ae23".lower(),
        "0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91".lower(),
        "0x2E71597b779F50D6e070662F0F0b53c63504B60C".lower()
    ]
    weth_df = trx_df[trx_df["to_normal"].isin(weth_contracts)].copy()
    trx_df = pd.concat([weth_df, trx_df]).drop_duplicates(keep=False)

    weth_df = eu.weth(weth_df, gas_coin, columns_out)
    vout = pd.concat([vout, weth_df])
    # END WETH ---------------------------------------------------------------------------------------------------------
    del weth_df
    # Open sea ---------------------------------------------------------------------------------------------------------
    opensea_contracts = ["0x921Fd42f147B26b51AA3c7fa3F2E2Ce7704c2858".lower(),
                         "0x00000000006c3852cbEf3e08E8dF289169EdE581".lower()]

    opensea_df = trx_df[trx_df["to_normal"].isin(opensea_contracts)].copy()
    trx_df = pd.concat([opensea_df, trx_df]).drop_duplicates(keep=False)
    opensea_df = nu.opensea(opensea_df, address, columns_out)

    vout = pd.concat([vout, opensea_df])
    # END Opensea-------------------------------------------------------------------------------------------------------
    del opensea_df
    # Blur -------------------------------------------------------------------------------------------------------------
    blur_contracts = ["0x000000000000Ad05Ccc4F10045630fb830B95127".lower(),
                      "0x39da41747a83aeE658334415666f3EF92DD0D541".lower(),
                      "0xb2ecfE4E4D61f8790bbb9DE2D1259B9e2410CEA5".lower()]

    blur_df = trx_df[trx_df["to_normal"].isin(blur_contracts)].copy()
    trx_df = pd.concat([blur_df, trx_df]).drop_duplicates(keep=False)
    blur_df = nu.blur(blur_df, address, columns_out)

    vout = pd.concat([vout, blur_df])

    # X2Y2 -------------------------------------------------------------------------------------------------------------
    x2y2 = nu.opensea(trx_df[trx_df["to_normal"] == '0x74312363e45DCaBA76c59ec49a7Aa8A65a67EeD3'.lower()].copy(),
                      address, columns_out)
    x2y2['Tag'] = 'X2Y2'

    trx_df = trx_df[trx_df["to_normal"] != '0x74312363e45DCaBA76c59ec49a7Aa8A65a67EeD3'.lower()]

    vout = pd.concat([vout, x2y2])
    # X2Y2 END ---------------------------------------------------------------------------------------------------------
    del x2y2
    # UNISWAP ----------------------------------------------------------------------------------------------------------

    uniswap_contracts = [
        "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff".lower(),
        "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower(),
        "0x4C60051384bd2d3C01bfc845Cf5F4b44bcbE9de5".lower(),
        "0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4".lower(),
    ]

    uniswap_df = trx_df[
        np.logical_or(
            trx_df["from_normal"].isin(uniswap_contracts),
            trx_df["to_normal"].isin(uniswap_contracts),
        )
    ]

    trx_df = pd.concat([uniswap_df, trx_df]).drop_duplicates(keep=False)
    uniswap_df = defi.uniswap(uniswap_df, address, columns_out)

    vout = pd.concat([vout, uniswap_df])

    # UNISWAP END ------------------------------------------------------------------------------------------------------
    del uniswap_df

    # Bridge --------------------------------------------------------------------------------------------------
    arb_df = trx_df[trx_df['to_normal'].isin(
        ['0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f', '0xaBEA9132b05A70803a4E85094fD0e1800777fBEF'])].copy()
    trx_df = pd.concat([arb_df, trx_df]).drop_duplicates(keep=False)

    arb_df.index = arb_df['timeStamp_normal']
    arb_df['From Amount'] = eu.calculate_value_eth(arb_df.value_normal)
    arb_df['From Amount'] *= -1
    arb_df['From Coin'] = 'ETH'
    arb_df['Fee'] = eu.calculate_gas(arb_df.gasPrice, arb_df.gasUsed_normal)
    arb_df['Tag'] = 'Bridge'

    arb_df = arb_df[[x for x in arb_df.columns if x in columns_out]]
    vout = pd.concat([vout, arb_df])
    # END arbitrum bridge ----------------------------------------------------------------------------------------------
    del arb_df
    # LOTM ASSETS ------------------------------------------------------------------------------------------------------

    lotm_contracts = ["0x5b1085136a811e55b2Bb2CA1eA456bA82126A376".lower(),
                      "0x3Bdca51226202Fc2a64211Aa35A8d95D61C6ca99".lower(),
                      "0x307b00dd72A29e0828b52947a2AdcD9e899167c9".lower(),
                      "0xDA98Cf8b3c6C4E05d568e6D38752cB6097414aB0".lower(),
                      "0x56e6F1BFFde5DCcd9A183585cE31f2902FC52707".lower()]

    lotm_df = trx_df[trx_df['to_normal'].isin(lotm_contracts)].copy()
    trx_df = pd.concat([lotm_df, trx_df]).drop_duplicates(keep=False)

    lotm_df = nu.LOTM(lotm_df, address, columns_out)
    vout = pd.concat([vout, lotm_df])
    # END LOTM ---------------------------------------------------------------------------------------------------------
    vout = vout.sort_index()
