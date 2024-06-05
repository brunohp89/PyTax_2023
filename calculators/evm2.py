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

pd.set_option('future.no_silent_downcasting', True)


def get_transactions_df(address, chain, scan_key=None):
    address = address.lower()
    columns_out = ['From', 'To', 'From Coin', 'To Coin', 'From Amount', 'To Amount', 'Fee', 'Fee Coin', 'Fee Fiat',
                   'Fiat', 'Fiat Price', 'Tag', 'Source', 'Notes']

    # Getting all transactions
    trx_df = eu.get_transactions_raw(address, chain, scan_key)
    # if trx_df[1].shape[0] == 0:
    # return trx_df[1]
    trx_df[1][columns_out] = None

    gas_coin = trx_df[0]
    trx_df = trx_df[1]

    trx_df['timeStamp_normal'] = trx_df['timeStamp_normal'].combine_first(trx_df['timeStamp']).combine_first(
        trx_df['timeStamp_internal'])

    trx_df[['value_normal', 'gas_normal', 'gasUsed_normal', 'gasPrice']] = trx_df[
        ['value_normal', 'gas_normal', 'gasUsed_normal', 'gasPrice']].fillna(0)

    trx_df['value_internal'] = trx_df['value_internal'].fillna(0)
    trx_df[['from_internal', 'from', 'to', 'to_internal', 'from_normal', 'to_normal']] = trx_df[
        ['from_internal', 'from', 'to', 'to_internal', 'from_normal', 'to_normal']].fillna('')

    trx_df_raw = trx_df.copy()

    vout = pd.DataFrame()

    # ------------------------------------------------------------------------------------------------------------------
    # Normal ETH transfers ---------------------------------------------------------------------------------------------
    eth_transfers_df = trx_df[np.logical_or(trx_df['functionName'].str.contains('transferOut', na=False),
                                            trx_df['input_normal'] == '0x')].copy()
    eth_transfers_df = pd.concat(
        [eth_transfers_df, trx_df[trx_df['functionName'].str.contains('anySwapOutNative', na=False)]]).drop_duplicates()
    trx_df = pd.concat([trx_df, eth_transfers_df]).drop_duplicates(keep=False)
    eth_transfers_df = eu.eth_transfers(eth_transfers_df, address, gas_coin, columns_out)

    vout = pd.concat([vout, eth_transfers_df])
    # END Normal ETH transfers -----------------------------------------------------------------------------------------
    del eth_transfers_df
    # Normal Internal ETH transfers ------------------------------------------------------------------------------------
    internal_df = trx_df[
        np.logical_and(pd.isna(trx_df['blockNumber_normal']), ~pd.isna(trx_df['blockNumber_internal']))]
    internal_df = internal_df[
        np.logical_and(pd.isna(internal_df['blockNumber']), pd.isna(internal_df['blockNumber_erc721']))]
    internal_df = internal_df[pd.isna(internal_df['blockNumber_erc1155'])]
    trx_df = pd.concat([trx_df, internal_df]).drop_duplicates(keep=False)

    internal_df[['timeStamp_normal', 'from_normal', 'to_normal', 'value_normal', 'gas_normal', 'gasUsed_normal']] = \
        internal_df[
            ['timeStamp_internal', 'from_internal', 'to_internal', 'value_internal', 'gas_internal',
             'gasUsed_internal']]

    vout = pd.concat([vout, eu.eth_transfers(internal_df, address, gas_coin, columns_out)])
    # LOVE ---------------------------------
    love_df = trx_df[trx_df['to_normal'].isin(
        ['0xFb063b1ae6471E6795d6ad1FC7f47c1cAb1f3422'.lower(),
         '0xb85EEb713b876A25f16604887cC6b8997ef1B9DD'.lower()])].copy()
    trx_df = pd.concat([trx_df, love_df]).drop_duplicates(keep=False)

    love_df = defi.love(love_df, columns_out)

    vout = pd.concat([vout, love_df])
    # LOVE END ---------------------------------------------------------------------------------------------------------
    del love_df

    # STARGATE ---------------------------------------------------------------------------------------------------------
    stargate_contracts = [
        "0x3052A0F6ab15b4AE1df39962d5DdEFacA86DaB47".lower(),  # Stargate Staking BSC
        "0x4a364f8c717cAAD9A442737Eb7b8A55cc6cf18D8".lower(),  # Stargate Router BSC
        "0xD4888870C8686c748232719051b677791dBDa26D".lower(),  # Stargate veSTG BSC
        "0xAF667811A7eDcD5B0066CD4cA0da51637DB76D09".lower(),  # Fee distributor BSC
        "0xbf22f0f184bccbea268df387a49ff5238dd23e40".lower(),  # Router ETH Arbitrum
        "0xea8dfee1898a7e0a59f7527f076106d7e44c2176".lower(),  # Stargate Staking  Arbitrum
        "0x53bf833a5d6c4dda888f69c22c88c9f356a41614".lower(),  # Stargate Router Arbitrum
        "0xb0d502e938ed5f4df2e681fe6e419ff29631d62b".lower(),  # Optimism
        "0x4dea9e918c6289a52cd469cac652727b7b412cd2".lower(),  # Optimism
        "0xe93685f3bba03016f02bd1828badd6195988d950".lower(),  # Optimism
        "0x81e792e5a9003cc1c8bf5569a00f34b65d75b017".lower(),  # Optimism
        "0xb49c4e680174e331cb0a7ff3ab58afc9738d5f8b".lower(),  # Optimism
        "0x86bb63148d17d445ed5398ef26aa05bf76dd5b59".lower(),  # Optimism
        "0x45A01E4e04F14f7A4a6702c74187c5F6222033cd".lower(),  # Polygon router
        "0x75dC8e5F50C8221a82CA6aF64aF811caA983B65f".lower(),  # Polygon relayer v2
        "0xce16F69375520ab01377ce7B88f5BA8C48F8D666".lower(),  # Base
        "0x50b6ebc2103bfec165949cc946d739d5650d7ae4".lower(),
        "0x06eb48763f117c7be887296cdcdfad2e4092739c".lower(),
        "0x2Eb9ea9dF49BeBB97e7750f231A32129a89b82ee".lower(),
        "0xA27A2cA24DD28Ce14Fb5f5844b59851F03DCf182".lower(),
        "0x6c33a7b29c8b012d060f3a5046f3ee5ac48f4780".lower(),
        "0x0A9f824C05A74F577A536A8A0c673183a872Dff4".lower(),
        "0x93e11BE33b25D562635558348DA0Dd5f74D8377B".lower(),
        "0x98e871aB1cC7e3073B6Cc1B661bE7cA678A33f7F".lower(),  # Harmony Bridge BSC
        "0x177d36dbe2271a4ddb2ad8304d82628eb921d790".lower()
    ]

    stargate_df = trx_df[np.logical_and(trx_df["to_normal"].isin(stargate_contracts),
                                        ~trx_df['functionName'].str.contains('approve', na=False))].copy()
    trx_df = pd.concat([stargate_df, trx_df]).drop_duplicates(keep=False)
    if stargate_df.shape[0] > 0:
        stargate_df = defi.stargate(stargate_df, address, gas_coin, columns_out)
        vout = pd.concat([vout, stargate_df])
        del stargate_df
    # Contract interactions Layer Zero (relayer V2)
    stargate_v2 = trx_df[trx_df["from_internal"].isin(stargate_contracts)]
    trx_df = pd.concat([trx_df, stargate_v2]).drop_duplicates(keep=False)
    if stargate_v2.shape[0] > 0:
        stargate_v2 = defi.layer_zero_v2(stargate_v2, gas_coin, columns_out)
        vout = pd.concat([vout, stargate_v2])
        del stargate_v2
    # STARGATE END -----------------------------------------------------------------------------------------------------
    # PANCAKE ---------------------------------------------------------------------------------------------------------
    pancake_contracts = [
        "0x45c54210128a065de780C4B0Df3d16664f7f859e".lower(),
        "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4".lower(),
        "0x5692DB8177a81A6c6afc8084C2976C9933EC1bAB".lower(),
        "0x10ED43C718714eb63d5aA57B78B54704E256024E".lower(),
        "0xa80240Eb5d7E05d3F250cF000eEc0891d00b51CC".lower(),
        "0xF15965AEBA71E4A9D2ED0D1aB568a6A3334F8e1F".lower(),
        "0x05fF2B0DB69458A0750badebc4f9e13aDd608C7F".lower()
    ]
    pancake_df = trx_df[trx_df["to_normal"].isin(pancake_contracts)].copy()
    trx_df = pd.concat([pancake_df, trx_df]).drop_duplicates(keep=False)
    if pancake_df.shape[0] >= 0:
        pancake_df = defi.pancake(pancake_df, address, columns_out, gas_coin)
        vout = pd.concat([vout, pancake_df])
        del pancake_df

    # SUSHI ---------------------------------------------------------------------------------------------------------
    sushi_contracts = [
        "0x7A4af156379f512DE147ed3b96393047226d923F".lower(),
        "0xd08b5f3e89F1e2d6b067e0A0cbdb094e6e41E77c".lower()
    ]
    sushi_df = trx_df[trx_df["to_normal"].isin(sushi_contracts)].copy()
    trx_df = pd.concat([sushi_df, trx_df]).drop_duplicates(keep=False)
    if sushi_df.shape[0] > 0:
        sushi_df = defi.sushi(sushi_df, columns_out, gas_coin)
        vout = pd.concat([vout, sushi_df])
        del sushi_df

    # UNISWAP ----------------------------------------------------------------------------------------------------------
    uniswap_contracts = [
        "0x5e325eda8064b456f4781070c0738d849c824258".lower(),
        "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower(),
        "0x4C60051384bd2d3C01bfc845Cf5F4b44bcbE9de5".lower(),
        "0xec8b0f7ffe3ae75d7ffab09429e3675bb63503e4".lower(),
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d".lower(),
        "0xc36442b4a4522e871399cd717abdd847ab11fe88".lower(),
        "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower(),
        "0xb555edF5dcF85f42cEeF1f3630a52A108E55A654".lower()
    ]

    uniswap_df = trx_df[
        np.logical_or(
            trx_df["from_normal"].isin(uniswap_contracts),
            trx_df["to_normal"].isin(uniswap_contracts),
        )
    ].copy()

    if uniswap_df.shape[0] > 0:
        trx_df = pd.concat([uniswap_df, trx_df]).drop_duplicates(keep=False)
        uniswap_df = defi.uniswap(uniswap_df, address, columns_out, gas_coin)
        vout = pd.concat([vout, uniswap_df])
        del uniswap_df

    # QUICK SWAP -------------------------------------------------------------------------------------------------------
    quick_contracts = ["0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff".lower()]
    quick_df = trx_df[trx_df["to_normal"].isin(quick_contracts)].copy()

    if quick_df.shape[0] > 0:
        trx_df = pd.concat([quick_df, trx_df]).drop_duplicates(keep=False)
        quick_df = defi.quick_swap(quick_df, address, columns_out, gas_coin)
        vout = pd.concat([vout, quick_df])
        del quick_df

    # THE GRAPH --------------------------------------------------------------------------------------------------------
    graph_contracts = [
        "0x00669a4cf01450b64e8a2a20e9b1fcb71e61ef03".lower()
    ]

    graph_df = trx_df[np.logical_or(
        trx_df["from_normal"].isin(graph_contracts),
        trx_df["to_normal"].isin(graph_contracts),
    )].copy()

    if graph_df.shape[0] > 0:
        trx_df = pd.concat([graph_df, trx_df]).drop_duplicates(keep=False)
        graph_df = defi.graph(graph_df, columns_out)
        vout = pd.concat([vout, graph_df])
        del graph_df

    # Galxe Claims -----------------------------------------------------------------------------------------------------
    galxe_contracts = ["0x2e42f214467f647Fe687Fd9a2bf3BAdDFA737465".lower()]
    galxe_df = trx_df[
        np.logical_and(trx_df['to_normal'].isin(galxe_contracts), trx_df['functionName'].str.contains('claim'))].copy()
    if galxe_df.shape[0] > 0:
        trx_df = pd.concat([galxe_df, trx_df]).drop_duplicates(keep=False)
        galxe_df.index = galxe_df['timeStamp_normal']
        galxe_df['To Coin'] = galxe_df['erc721_complete_name']
        galxe_df['To Amount'] = 1
        galxe_df[['Tag', 'Notes']] = ['Movement', 'NFT']

        galxe_df['Fee'] = eu.calculate_gas(galxe_df['gasPrice_erc721'], galxe_df['gasUsed_erc721'])

        galxe_df = galxe_df[[x for x in galxe_df.columns if x in columns_out]]
        galxe_df = galxe_df.sort_index()

        vout = pd.concat([vout, galxe_df])
        del galxe_df

    # WETH -------------------------------------------------------------------------------------------------------------
    # WETH wrapping
    weth_contracts = [
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(),
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
        "0x5c7f8a570d578ed84e63fdfa7b1ee72deae1ae23".lower(),
        "0x5AEa5775959fBC2557Cc8789bC1bf90A239D9a91".lower(),
        "0x2E71597b779F50D6e070662F0F0b53c63504B60C".lower(),
        "0x4200000000000000000000000000000000000006".lower()
    ]
    weth_df = trx_df[trx_df["to_normal"].isin(weth_contracts)].copy()
    trx_df = pd.concat([weth_df, trx_df]).drop_duplicates(keep=False)

    weth_df = eu.weth(weth_df, gas_coin, columns_out)
    vout = pd.concat([vout, weth_df])
    # END WETH ---------------------------------------------------------------------------------------------------------
    del weth_df
    # Open sea ---------------------------------------------------------------------------------------------------------
    opensea_contracts = ["0x921Fd42f147B26b51AA3c7fa3F2E2Ce7704c2858".lower(),
                         "0x00000000006c3852cbEf3e08E8dF289169EdE581".lower(),
                         "0x00000000000000adc04c56bf30ac9d3c0aaf14dc".lower(),
                         "0x00000000000001ad428e4906ae43d8f9852d0dd6".lower(),
                         "0x00005ea00ac477b1030ce78506496e8c2de24bf5".lower(),
                         "0x0000000000c2d145a2526bD8C716263bFeBe1A72".lower()]

    opensea_df = trx_df[trx_df["to_normal"].isin(opensea_contracts)].copy()
    if opensea_df.shape[0] > 0:
        trx_df = pd.concat([opensea_df, trx_df]).drop_duplicates(keep=False)
        opensea_df = nu.opensea(opensea_df, address, columns_out)

        vout = pd.concat([vout, opensea_df])
        del opensea_df
    # END Opensea-------------------------------------------------------------------------------------------------------
    # Blur -------------------------------------------------------------------------------------------------------------
    blur_contracts = ["0x000000000000Ad05Ccc4F10045630fb830B95127".lower(),
                      "0x39da41747a83aeE658334415666f3EF92DD0D541".lower(),
                      "0xb2ecfE4E4D61f8790bbb9DE2D1259B9e2410CEA5".lower(),
                      "0x0000000000a39bb272e79075ade125fd351887ac".lower()]

    blur_df = trx_df[trx_df["to_normal"].isin(blur_contracts)].copy()
    if blur_df.shape[0] > 0:
        trx_df = pd.concat([blur_df, trx_df]).drop_duplicates(keep=False)
        blur_df = nu.blur(blur_df, address, columns_out)

        vout = pd.concat([vout, blur_df])

    # X2Y2 -------------------------------------------------------------------------------------------------------------
    x2y2 = trx_df[trx_df["to_normal"] == '0x74312363e45DCaBA76c59ec49a7Aa8A65a67EeD3'.lower()].copy()
    if x2y2.shape[0] > 0:
        x2y2 = nu.opensea(x2y2,
                          address, columns_out)
        x2y2['Tag'] = 'X2Y2'

        trx_df = trx_df[trx_df["to_normal"] != '0x74312363e45DCaBA76c59ec49a7Aa8A65a67EeD3'.lower()]

        vout = pd.concat([vout, x2y2])
        del x2y2
    # X2Y2 END ---------------------------------------------------------------------------------------------------------
    # Bridge --------------------------------------------------------------------------------------------------
    arb_df = trx_df[trx_df['to_normal'].isin(
        ['0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f'.lower(),
         '0xabea9132b05a70803a4e85094fd0e1800777fbef'.lower(),
         '0x32400084C286CF3E17e7B677ea9583e60a000324'.lower()])].copy()
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
                      "0x56e6F1BFFde5DCcd9A183585cE31f2902FC52707".lower(),
                      "0x41e4a828630dD4729fC010E9483CD900bb37FC79".lower()]

    lotm_df = trx_df[trx_df['to_normal'].isin(lotm_contracts)].copy()
    trx_df = pd.concat([lotm_df, trx_df]).drop_duplicates(keep=False)

    lotm_df = nu.LOTM(lotm_df, address, columns_out)
    vout = pd.concat([vout, lotm_df])
    # END LOTM ---------------------------------------------------------------------------------------------------------
    del lotm_df
    # SANDBOX claim prizes ---------------------------------------------------------------------------------------------
    sandbox_contracts = ["0xa21342f796996954284b8dc6aae7ecbf8f83a9e4".lower(),
                         "0x4AB071C42C28c4858C4BAc171F06b13586b20F30".lower(),
                         "0x214d52880b1e4E17d020908cd8EAa988FfDD4020".lower()]

    tsb_df = trx_df[np.logical_or(trx_df['to_normal'].isin(sandbox_contracts), trx_df['from'].isin(sandbox_contracts))].copy()
    trx_df = pd.concat([tsb_df, trx_df]).drop_duplicates(keep=False)

    tsb_df = nu.the_sandbox(tsb_df, columns_out)

    vout = pd.concat([vout, tsb_df])
    # END SANDBOX claim prizes -----------------------------------------------------------------------------------------
    del tsb_df
    # ENS NAME WRAPPER -------------------------------------------------------------------------------------------------
    ens_df = trx_df[trx_df['to_normal'] == '0x253553366da8546fc250f225fe3d25d0c782303b'].copy()
    trx_df = pd.concat([ens_df, trx_df]).drop_duplicates(keep=False)

    ens_df = eu.ens(ens_df, columns_out)
    vout = pd.concat([vout, ens_df])

    vout = vout.sort_index()
    # ENS NAME WRAPPER END ---------------------------------------------------------------------------------------------
    del ens_df
    # WARM WALLET DELEGATION -------------------------------------------------------------------------------------------
    warm_df = trx_df[trx_df['to_normal'] == '0xc3aa9bc72bd623168860a1e5c6a4530d3d80456c'].copy()
    trx_df = pd.concat([warm_df, trx_df]).drop_duplicates(keep=False)

    warm_df.index = warm_df['timeStamp_normal']
    warm_df['Fee'] = eu.calculate_gas(warm_df.gasPrice, warm_df.gasUsed_normal)
    warm_df['Tag'] = 'Warm Wallet Delegation'
    warm_df = warm_df[[x for x in warm_df.columns if x in columns_out]]
    warm_df = warm_df.sort_index()

    vout = pd.concat([vout, warm_df])

    del warm_df

    # BASE NFTS --------------------------------------------------------------------------------------------------------
    base_df = trx_df[np.logical_or(trx_df['to_normal'] == '0xD4307E0acD12CF46fD6cf93BC264f5D5D1598792'.lower(),
                                   trx_df[
                                       'contractAddress_erc721'] == '0xD4307E0acD12CF46fD6cf93BC264f5D5D1598792'.lower())].copy()
    trx_df = pd.concat([base_df, trx_df]).drop_duplicates(keep=False)

    base_df.index = base_df['timeStamp_normal']
    base_df['Fee'] = eu.calculate_gas(base_df.gasPrice, base_df.gasUsed_normal)
    base_df['To Coin'] = base_df['erc721_complete_name'].combine_first(base_df['erc1155_complete_name'])
    base_df['To Amount'] = 1
    base_df['From Amount'] = eu.calculate_value_eth(base_df['value_normal'])
    base_df['From Amount'] *= -1
    base_df['From Coin'] = 'ETH'

    base_df[['Tag', 'Notes']] = ['Movement', 'Base NFT']
    base_df = base_df[[x for x in base_df.columns if x in columns_out]]
    vout = pd.concat([vout, base_df])

    del base_df

    # STAND WITH CRYPTO-------------------------------------------------------------------------------------------------
    swc_df = trx_df[trx_df['to_normal'] == '0x9D90669665607F08005CAe4A7098143f554c59EF'.lower()].copy()
    trx_df = pd.concat([swc_df, trx_df]).drop_duplicates(keep=False)

    swc_df.index = swc_df['timeStamp_normal']
    swc_df['Fee'] = eu.calculate_gas(swc_df.gasPrice, swc_df.gasUsed_normal)
    swc_df['To Coin'] = swc_df['erc721_complete_name']
    swc_df['To Amount'] = 1
    swc_df['From Amount'] = eu.calculate_value_eth(swc_df['value_normal'])
    swc_df['From Amount'] *= -1
    swc_df['From Coin'] = 'ETH'

    swc_df[['Tag', 'Notes']] = ['Movement', 'Stand With Crypto NFT']
    swc_df = swc_df[[x for x in swc_df.columns if x in columns_out]]
    vout = pd.concat([vout, swc_df])

    del swc_df

    # FUNDROP NFTS --------------------------------------------------------------------------------------------------------
    fun_df = trx_df[trx_df['to_normal'] == '0x0000000000664ceffed39244a8312bD895470803'.lower()].copy()
    trx_df = pd.concat([fun_df, trx_df]).drop_duplicates(keep=False)

    fun_df.index = fun_df['timeStamp_normal']
    fun_df['Fee'] = eu.calculate_gas(fun_df.gasPrice, fun_df.gasUsed_normal)
    fun_df['To Coin'] = fun_df['erc721_complete_name']
    fun_df['To Amount'] = 1

    fun_df[['Tag', 'Notes']] = ['Movement', 'FunDrop NFT']
    fun_df = fun_df[[x for x in fun_df.columns if x in columns_out]]
    vout = pd.concat([vout, fun_df])

    del fun_df

    # SERUM CITY -------------------------------------------------------------------------------------------------------
    serum_df = trx_df[trx_df['to_normal'] == '0xce2822a740e37f0B3d9F3e098c3d850d8C0634c3'.lower()].copy()
    trx_df = pd.concat([serum_df, trx_df]).drop_duplicates(keep=False)

    serum_df.index = serum_df['timeStamp_normal']
    serum_df['Fee'] = eu.calculate_gas(serum_df.gasPrice, serum_df.gasUsed_normal)

    serum_df[['Tag', 'Notes']] = ['Movement', 'Serum City Pass Mint']
    serum_df = serum_df[[x for x in serum_df.columns if x in columns_out]]
    vout = pd.concat([vout, serum_df])

    del serum_df
    # ONE INCH ---------------------------------------------------------------------------------------------------------
    one_contracts = [
        "0x1111111254eeb25477b68fb85ed929f73a960582".lower(),
        "0x2eb393fbac8aaa16047d4242033a25486e14f345".lower(),
        "0x9d4eb7189cd57693c3d01f35168715e1e589cea8".lower(),
        "0x95e2769aca43a1d4febcfa153022259fc1e49548".lower(),
        "0x4bc3e539aaa5b18a82f6cd88dc9ab0e113c63377".lower(),
        "0x1111111254760F7ab3F16433eea9304126DCd199".lower(),
        "0xd89adc20c400b6c45086a7f6ab2dca19745b89c2".lower(),
        "0xb63aae6c353636d66df13b89ba4425cfe13d10ba".lower()
    ]
    one_df = trx_df[np.logical_or(trx_df['to'].isin(one_contracts), trx_df["to_normal"].isin(one_contracts))].copy()
    one_df = pd.concat([one_df, trx_df[trx_df['from'].isin(one_contracts)]])
    one_df = one_df.drop_duplicates()
    trx_df = pd.concat([one_df, trx_df]).drop_duplicates(keep=False)
    if one_df.shape[0] > 0:
        one_df = defi.one_inch(one_df, address, columns_out, gas_coin)
        vout = pd.concat([vout, one_df])
        del one_df

    # Gitcoin Passport -------------------------------------------------------------------------------------------------
    gitcoin_contracts = ["0xa8eD4d2C3f6f98A55cdDEd97C5aE9B932B0633A4".lower()]
    gitcoin_df = trx_df[trx_df['to_normal'].isin(gitcoin_contracts)].copy()
    trx_df = pd.concat([gitcoin_df, trx_df]).drop_duplicates(keep=False)
    if gitcoin_df.shape[0] > 0:
        gitcoin_df['From Amount'] = eu.calculate_value_eth(gitcoin_df['value_normal'])
        gitcoin_df['From Amount'] *= -1
        gitcoin_df['From Coin'] = gas_coin
        gitcoin_df.index = gitcoin_df['timeStamp_normal']
        gitcoin_df['Fee'] = eu.calculate_gas(gitcoin_df['gasPrice'], gitcoin_df['gasUsed_normal'])
        gitcoin_df[['Tag', 'Notes']] = ['Movement', 'Gitcoin passport']
        gitcoin_df = gitcoin_df[[x for x in gitcoin_df.columns if x in columns_out]]
        gitcoin_df = gitcoin_df.sort_index()
        vout = pd.concat([vout, gitcoin_df])
        del gitcoin_df

    # OP delegations ---------------------------------------------------------------------------------------------------
    trx_df.loc[np.logical_and(trx_df['to_normal'] == '0x4200000000000000000000000000000000000042'.lower(),
                              trx_df['functionName'].str.contains('delegate')), 'functionName'] = 'approv'

    # YEARN FINANCE ----------------------------------------------------------------------------------------------------
    year_contracts = ["0x7D2382b1f8Af621229d33464340541Db362B4907".lower()]
    yearn_df = trx_df[trx_df['to_normal'].isin(year_contracts)].copy()
    trx_df = pd.concat([yearn_df, trx_df]).drop_duplicates(keep=False)

    if yearn_df.shape[0] > 0:
        yearn_df = defi.yearn(yearn_df, columns_out)
        vout = pd.concat([vout, yearn_df])
        del yearn_df

    # 0x Exchange ------------------------------------------------------------------------------------------------------
    zerox_contracts = ["0xDEF1ABE32c034e558Cdd535791643C58a13aCC10".lower()]
    zerox_df = trx_df[trx_df['to_normal'].isin(zerox_contracts)].copy()
    trx_df = pd.concat([zerox_df, trx_df]).drop_duplicates(keep=False)

    if zerox_df.shape[0] > 0:
        zerox_df = defi.zerox(zerox_df, address, gas_coin, columns_out)
        vout = pd.concat([vout, zerox_df])
        del zerox_df

    # Optimism tasks Claims --------------------------------------------------------------------------------------------
    op_contracts = ["0x04bA6cf3c5AA6D4946F5B7f7ADF111012a9fAC65".lower(),
                    "0x2335022c740d17c2837f9C884Bfe4fFdbf0A95D5".lower()]
    op_df = trx_df[
        np.logical_and(trx_df['to_normal'].isin(op_contracts), trx_df['functionName'].str.contains('mint'))].copy()
    if op_df.shape[0] > 0:
        trx_df = pd.concat([op_df, trx_df]).drop_duplicates(keep=False)
        op_df = nu.optimism_quests(op_df, gas_coin, columns_out)
        vout = pd.concat([vout, op_df])
        del op_df

    # DECENTRALAND MARKETPLACE -----------------------------------------------------------------------------------------
    mana_contracts = ["0x214ffC0f0103735728dc66b61A22e4F163e275ae".lower()]
    mana_df = trx_df[trx_df['to_normal'].isin(mana_contracts)].copy()
    mana_df = mana_df[mana_df['functionName'].str.contains('buy')]
    if mana_df.shape[0] > 0:
        trx_df = pd.concat([mana_df, trx_df]).drop_duplicates(keep=False)
        mana_df.index = mana_df['timeStamp_normal']
        mana_df['From Amount'] = eu.calculate_value_token(mana_df['value'].fillna(0), mana_df['tokenDecimal'].fillna(0))
        mana_df['From Amount'] *= -1
        mana_df['From Coin'] = 'MANA'
        mana_df['To Coin'] = mana_df['erc721_complete_name']
        mana_df['To Amount'] = 1
        mana_df['Fee'] = eu.calculate_gas(mana_df['gasPrice_erc721'], mana_df['gasUsed_erc721'])

        mana_df = mana_df[[x for x in mana_df.columns if x in columns_out]]
        mana_df = mana_df.sort_index()

        mana_grouped = mana_df.groupby([mana_df.index]).agg({'From Amount':'sum'}).reset_index()

        mana_df = pd.merge(mana_grouped, mana_df, left_on='timeStamp_normal', right_index=True, suffixes=('-',''))
        mana_df['From Amount'] = mana_df['From Amount-']
        mana_df.index = mana_df['timeStamp_normal']
        mana_df = mana_df.drop(['From Amount-', 'timeStamp_normal'], axis=1)
        mana_df = mana_df.drop_duplicates()

        vout = pd.concat([vout, mana_df])
        del mana_df
    # Normal ERC20 transfers -------------------------------------------------------------------------------------------
    erc20_transfers_df = trx_df[~pd.isna(trx_df['tokenSymbol'])].copy()
    erc20_transfers_df = erc20_transfers_df[
        np.logical_or(pd.isna(erc20_transfers_df['blockNumber_normal']),
                      erc20_transfers_df['value_normal'].isin(['0', 0]))]
    erc20_transfers_df = erc20_transfers_df[
        np.logical_or(pd.isna(erc20_transfers_df['value_internal']), erc20_transfers_df['value_normal'].isin(['0', 0]))]
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
    erc721_transfers_df = pd.concat(
        [erc721_transfers_df, trx_df[trx_df['functionName'].str.contains('safeTransferFrom', na=False)]])
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

    if trx_df.shape[0] > 0:
        print("ATTENZIONE: TRANSAZIONI MANCANTI")
    # ------------------------------------------------------------------------------------------------------------------
    if f'{chain}_{address}.csv' in os.listdir():
        vout = pd.concat([pd.read_csv(f'{chain}_{address}.csv', index_col='Timestamp', parse_dates=True), vout])

    vout['Fee Coin'] = gas_coin
    vout['Fiat'] = 'EUR'
    vout = vout.sort_index()

    eth_prices = Prices()

    temp_nft = vout[np.logical_or(vout['Notes'].str.contains('NFT', na=False), vout['Tag'] == 'LOTM')].copy()
    vout.loc[np.logical_or(vout['Notes'].str.contains('NFT', na=False), vout['Tag'] == 'LOTM'), 'New Tag'] = '!'

    vout.loc[vout['From Coin'].str.contains('->', na=False), ['From Coin', 'From Amount']] = None
    vout.loc[vout['To Coin'].str.contains('->', na=False), ['To Coin', 'To Amount']] = None

    vout = tx.price_transactions_df(vout, eth_prices)

    vout.loc[vout['New Tag'] == '!', ['From Coin', 'From Amount', 'To Coin', 'To Amount']] = temp_nft[
        ['From Coin', 'From Amount', 'To Coin', 'To Amount']]
    vout['Fee'] = vout['Fee'].infer_objects(copy=False).fillna(0)
    vout['Fee Fiat'] = vout['Fee Fiat'].infer_objects(copy=False).fillna(0)

    vout = vout.drop('New Tag', axis=1)

    vout['Fee'] = vout['Fee'].astype(float)
    vout['From Amount'] = vout['From Amount'].astype(float)
    vout['To Amount'] = vout['To Amount'].astype(float)

    vout['Source'] = f'{gas_coin}-{address[0:10]}'

    vout = vout.sort_index()
    return vout
