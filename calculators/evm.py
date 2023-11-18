import requests
import pandas as pd
import datetime as dt
import numpy as np
from PricesClass import Prices
import tax_library as tx
import os

scam = [
    'cronosclassic.com',
    'CRONOSCLASSIC.COM',
    '0xab57aef3601cad382aa499a6ae2018a69aad9cf0',
    '0xf3822314b333cbd7a36753b77589afbe095df1ba',
    'CRONOSCLASSIC.COM',
    'Valorant Mint Pass Official - 8',
    'Walking Mushroom Mint Box Limited - 509',
    '0xe592427a0aece92de3edee1f18e0157c05861564',
    'Overwatch SZN1 Mint Pass Limited - 288',
    'KRDH Official - 257',
    'YUGA: Gucci Merch PASS - 35',
    'Crypto Diverse Items - 258',
    'Gods and Titans Mint Pass Original - 103',
    'Claim USDC at https://cusdcs.eth.li',
    'LOOKSDROP.COM',
    '995$ Visit USDTReward.com to claim',
    'FCKIT', 'DBT', 'MKS', 'MGRT', 'XEC', 'BSC', 'BSC-USD', 'XEN', '0Army.io',
    'BEP-20 TOKEN', 'BEP-20', 'AI-A', 'https://wincoin.win/',
    'Claim USDC at https://cusdcs.eth.li', 'wHEX', 'wSNX',
    'Visit sufek.com and claim special rewards',
    '$ Free Claim and Play', 'BalancerV2.com', 'TheSandBox.PRO',
    'Visit https://jbonus.site to claim reward',
    'Claim USDC/WETH at https://USDCpool.site', 'vanity-address.io', 'lrETH',
    'DogX.AI NFT', 'wRDNT', '$ sUSD', 'wSNX', 'sUSD [Synthetix.cc]',
    '! !$100,000 SHIB #1 - 0', 'Life Goes On 846 Meta - 273',
    'BlurredApeYachtClub - 0', 'ULand Genesis Item - 1', '$1000 SPEND REWARDS - 0',
    'APE NFT TICKETS - 1781', '$$30,000 BONE - 0', 'UnisMeta 1671392971464 - 153',
    '$1000 REWARDS - 0', 'StarBored - 8014', 'BAYC Airdrop - 13',
    '! !$100,000 BONE - 0', 'dYdX Exchange Event - 202', 'MATIC BONUS - 5838',
    'Milady NFT Gift - 1', '$ELO Coin Launch Party - 0', '!1 0PENSEA V0UCHER - 0',
    '2,000 USDT Reward - 0', 'VWIN - 872448',
    '10 000 USD FOR FREE - 10034870321424364224204695929735558941153547521495712359208821587756539969537',
    'TheSandBox.PRO', '$1000 Rewards - 0',
    '$$$AIRDROP 0PENSEA - 45975694115932666297700772423167124246419737974611922237769215783254357966850',
    'Star Fighter Club QX - 31', '1000$ Reward - 0', 'ApeCoin NFT - 434',
    'SavageNation LW Tournament Pass Originals - 24', 'The Sougen Genesis Pass - 1',
    '$2000 USDT Airdrop - 1', '1099$ USDC - 83215', '5000 USDC - 0', 'DarkoZoo - 9101',
    'The QPT Originals - 336', 'AVALANCE NFT TICKETS - 9335', 'SHIBPOOL.COM - 1',
    '4,651$ SHIB - 0', 'BAYC Airdrop - 12', '2500 USDT by ETHERSCAN x METAWIN - 0',
    '1 stETH - 0', '$!$ 30,000 BONE - 0', 'CyberStrife Axe Drop 1 - 234',
    '5000 USDC Voucher - 0', 'BLUR EVENT - 2009', 'Anatomic WCE Apes - 532',
    'Kodao-G Membership - 28', 'Wiz GGY Box - 131', 'Magma CO Pass - 28',
    'Uniswap Summer Event - 176', 'The Mutants Return Land Pass Officials - 1',
    'Warning - 31093', 'LIDO WHITELIST - 1', '$1000 USDC - 0', 'Super Boys Orignal - 1',
    '5 ETH Voucher by Base - 0', 'Otherdeed Coda Key - 2', 'APE NFT TICKETS - 8432',
    'Nested Box BJ Club - 36', 'LIDO WHITELIST - 1196',
    '1 WETH - 560631', '0x794a61358d6845594f94dc1db02a252b5b4814ad',
    'RTFKT - MNLTH CVD X - 199', 'LIDO NFT TICKETS - 1',
    'MNEB', 'VERA', 'EVER', 'BSCTOKEN', 'AIR', 'BNBW', 'ABFIN', 'Zepe.io'
]


# The transactions on Crypto.org chain have to be extracted manually, refer to the example file
def get_crypto_dot_org_transactions(address):
    address = address.lower()
    if address not in os.listdir('cryptodotorg'):
        print("No files for crypto.org found")
        return pd.DataFrame(columns=['From', 'To', 'From Coin', 'To Coin', 'From Amount', 'To Amount', 'Fee',
                                     'Fee Coin', 'Fee Fiat', 'Fiat', 'Fiat Price', 'Tag', 'Source', 'Notes'])
    cronos_files = [
        os.path.join(os.path.abspath(f'cryptodotorg/{address}'), x)
        for x in os.listdir(os.path.abspath(f'cryptodotorg/{address}'))
        if "automatico" not in x
    ]
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

    final_df['From'] = [k.lower() for k in final_df['From']]
    final_df['To'] = [k.lower() for k in final_df['To']]

    final_df.loc[final_df['To'] != address, 'From Amount'] *= -1

    final_df.loc[final_df['From Amount'] < 0, 'From Amount'] = final_df.loc[final_df['From Amount'] < 0, 'From Amount']
    final_df.loc[final_df['From Amount'] > 0, 'To Amount'] = final_df.loc[final_df['From Amount'] > 0, 'From Amount']

    final_df.loc[final_df['From Amount'] < 0, 'From Coin'] = final_df.loc[final_df['From Amount'] < 0, 'From Coin']
    final_df.loc[final_df['From Amount'] > 0, 'To Coin'] = final_df.loc[final_df['From Amount'] > 0, 'From Coin']

    final_df.loc[final_df['From Amount'] > 0, ['From Amount', 'From Coin']] = None

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


def get_transactions_df(address, scan_key, chain):
    address = address.lower()

    if chain == 'eth-mainnet':
        main_url = 'https://api.etherscan.io/'
        gas_coin = 'ETH'
    elif chain == 'bsc-mainnet':
        main_url = 'https://api.bscscan.com/'
        gas_coin = 'BNB'
    elif chain == 'arb-mainnet':
        main_url = 'https://api.arbiscan.io/'
        gas_coin = 'ETH'
    elif chain == 'op-mainnet':
        main_url = 'https://api-optimistic.etherscan.io/'
        gas_coin = 'ETH'
    elif chain == 'pol-mainnet':
        main_url = 'https://api.polygonscan.com/'
        gas_coin = 'MATIC'
    elif chain == 'cro-mainnet':
        main_url = 'https://api.cronoscan.com/'
        gas_coin = 'CRO'
    elif chain == 'base-mainnet':
        main_url = 'https://api.basescan.org/'
        gas_coin = 'ETH'
    else:
        raise AttributeError("chain not recognized")

    url = f"{main_url}api?module=account&action=txlist&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={scan_key}"
    response = requests.get(url)
    normal = pd.DataFrame(response.json().get("result"))
    if normal.shape[0] == 0:
        print("No transactions found")
        return pd.DataFrame(columns=['From', 'To', 'From Coin', 'To Coin', 'From Amount', 'To Amount', 'Fee',
                                     'Fee Coin', 'Fee Fiat', 'Fiat', 'Fiat Price', 'Tag', 'Source', 'Notes'])

    url = f"{main_url}api?module=account&action=tokennfttx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={scan_key}"
    response = requests.get(url)
    erc721 = pd.DataFrame(response.json().get("result"))
    if erc721.shape[0] > 0:
        erc721['erc721_complete_name'] = erc721['tokenName'] + ' - ' + erc721['tokenID']

    erc1155 = pd.DataFrame()
    if chain != 'arb-mainnet':
        url = f"{main_url}api?module=account&action=token1155tx&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={scan_key}"
        response = requests.get(url)
        erc1155 = pd.DataFrame(response.json().get("result"))
        if erc1155.shape[0] > 0:
            erc1155['erc1155_complete_name'] = erc1155['tokenName'] + ' - ' + erc1155['tokenID']
    if chain == 'arb-mainnet':
        erc1155 = pd.DataFrame()

    url = f"{main_url}api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=9999999999999999999&sort=asc&apikey={scan_key}"
    response_internal = requests.get(url)
    internal = pd.DataFrame(response_internal.json().get("result"))

    url = f"{main_url}api?module=account&action=tokentx&address={address}&startblock=0&endblock=999999999999&sort=asc&apikey={scan_key}"
    response = requests.get(url)
    erc20 = pd.DataFrame(response.json().get("result"))
    if erc20.shape[0] > 0:
        erc20.loc[erc20['tokenSymbol'] == 'BSC-USD', 'tokenSymbol'] = 'USDT'
        erc20.loc[erc20['tokenSymbol'] == 'USDC.e', 'tokenSymbol'] = 'USDCE'
        erc20['from'] = [k.lower() for k in erc20['from']]
        for timestamp in erc20['timeStamp'].unique():
            if list(erc20.loc[erc20['timeStamp'] == timestamp, 'from'])[
                0] == '0x4aef1fd68c9d0b17d85e0f4e90604f6c92883f18':
                erc20.loc[erc20['timeStamp'] == timestamp, 'value'] = str(
                    int(list(erc20.loc[erc20['timeStamp'] == timestamp, 'value'])[0]) * len(
                        list(erc20.loc[erc20['timeStamp'] == timestamp, 'value'])))
        erc20 = erc20.drop_duplicates()

    if internal.shape[0] == 0:
        internal = pd.DataFrame(data=None, columns=['blockNumber', 'timeStamp', 'hash', 'from', 'to', 'value',
                                                    'contractAddress', 'input', 'type', 'gas', 'gasUsed', 'traceId',
                                                    'isError', 'errCode'])

    if erc721.shape[0] == 0:
        erc721 = pd.DataFrame(data=None, columns=['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from',
                                                  'contractAddress', 'to', 'tokenID', 'tokenName', 'tokenSymbol',
                                                  'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed',
                                                  'cumulativeGasUsed', 'input', 'confirmations',
                                                  'erc721_complete_name'])

    if erc1155.shape[0] == 0:
        erc1155 = pd.DataFrame(data=None, columns=['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash',
                                                   'transactionIndex', 'gas', 'gasPrice', 'gasUsed',
                                                   'cumulativeGasUsed',
                                                   'input', 'contractAddress', 'from', 'to', 'tokenID', 'tokenValue',
                                                   'tokenName', 'tokenSymbol', 'confirmations',
                                                   'erc1155_complete_name'])

    if erc20.shape[0] == 0:
        erc20 = pd.DataFrame(data=None, columns=['blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash', 'from',
                                                 'contractAddress', 'to', 'value', 'tokenName', 'tokenSymbol',
                                                 'tokenDecimal', 'transactionIndex', 'gas', 'gasPrice', 'gasUsed',
                                                 'cumulativeGasUsed', 'input', 'confirmations'])

    # Scam BUSD tokens
    erc20['to'] = [k.lower() for k in erc20['to']]
    erc20 = erc20[~erc20['to'].isin(
        ['0x552cacece6b9448a6bc5a91cf498ddfd4b6886cc'.lower(), '0x5525abFEB2c802C540307a29F2B5958ca9Ca86cC'.lower()])]

    trx_df = pd.merge(pd.merge(pd.merge(pd.merge(
        normal, internal, how="outer", on="hash", suffixes=("_normal", "_internal")
    ), erc20, how="outer", on="hash", suffixes=("", "_erc20")), erc721, how="outer", on="hash",
        suffixes=("", "_erc721"))
        , erc1155, how="outer", on="hash", suffixes=("", "_erc1155")).drop_duplicates()

    trx_df.loc[trx_df['isError_normal'] == '1', ['value_normal', 'value']] = '0'

    nfts = erc721['erc721_complete_name'].tolist()
    nfts.extend(erc1155['erc1155_complete_name'])
    nfts = list(set(nfts))

    # Regular ETH transfers

    regular_transfers = trx_df[trx_df['input_normal'] == '0x']
    regular_transfers = regular_transfers[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'value_normal', 'gasUsed_normal', 'gasPrice']]
    regular_transfers['tokenName'] = gas_coin
    regular_transfers['kind'] = f'{gas_coin} Transfer'
    trx_df = trx_df[trx_df['input_normal'] != '0x']

    regular_transfers['to_normal'] = regular_transfers['to_normal'].apply(lambda x: x.lower())
    final_df = regular_transfers.copy()
    final_df.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin', 'Kind']
    final_df['To Amount'] = [int(x) / 10 ** 18 for x in final_df['To Amount']]
    final_df['From Amount'] = final_df['From Coin'] = None

    final_df.loc[final_df['To'] != address, 'To Amount'] *= -1
    final_df.loc[final_df['To Amount'] < 0, 'From Amount'] = final_df.loc[final_df['To Amount'] < 0, 'To Amount']
    final_df.loc[final_df['To Amount'] < 0, 'From Coin'] = final_df.loc[final_df['To Amount'] < 0, 'To Coin']
    final_df.loc[final_df['To Amount'] < 0, 'To Coin'] = None
    final_df.loc[final_df['To Amount'] < 0, 'To Amount'] = None

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===========================================STARGATE================================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    stargate_contracts = ['0x3052A0F6ab15b4AE1df39962d5DdEFacA86DaB47'.lower(),  # Stargate Staking BSC
                          '0x4a364f8c717cAAD9A442737Eb7b8A55cc6cf18D8'.lower(),  # Stargate Router BSC
                          '0xD4888870C8686c748232719051b677791dBDa26D'.lower(),  # Stargate veSTG BSC
                          '0xAF667811A7eDcD5B0066CD4cA0da51637DB76D09'.lower(),  # Fee distributor BSC
                          '0xbf22f0f184bccbea268df387a49ff5238dd23e40'.lower(),  # Router ETH Arbitrum
                          '0xea8dfee1898a7e0a59f7527f076106d7e44c2176'.lower(),  # Stargate Staking  Arbitrum
                          '0x53bf833a5d6c4dda888f69c22c88c9f356a41614'.lower(),  # Stargate Router Arbitrum
                          '0xb0d502e938ed5f4df2e681fe6e419ff29631d62b'.lower(),  # Optimism
                          '0x4dea9e918c6289a52cd469cac652727b7b412cd2'.lower(),  # Optimism
                          '0xe93685f3bba03016f02bd1828badd6195988d950'.lower(),  # Optimism
                          '0x81e792e5a9003cc1c8bf5569a00f34b65d75b017'.lower(),  # Optimism
                          '0xb49c4e680174e331cb0a7ff3ab58afc9738d5f8b'.lower(),  # Optimism
                          '0x86bb63148d17d445ed5398ef26aa05bf76dd5b59'.lower(),  # Optimism
                          '0x45A01E4e04F14f7A4a6702c74187c5F6222033cd'.lower(),  # Polygon router
                          '0x75dC8e5F50C8221a82CA6aF64aF811caA983B65f'.lower(),  # Polygon relayer v2
                          '0xce16F69375520ab01377ce7B88f5BA8C48F8D666'.lower(),  # Base
                          '0x50b6ebc2103bfec165949cc946d739d5650d7ae4'.lower(),
                          '0x06eb48763f117c7be887296cdcdfad2e4092739c'.lower()]

    stargate_df = trx_df[trx_df['to_normal'].isin(stargate_contracts)].copy()
    stargate_df = stargate_df[stargate_df['functionName'] != 'approve(address _spender, uint256 _value)']
    trx_df = pd.concat([stargate_df, trx_df]).drop_duplicates(keep=False)
    vout = pd.DataFrame(
        columns=['Timestamp', 'Gasused', 'Gasprice', 'To Amount', 'To Coin', 'Kind', 'From Amount', 'From Coin', 'From',
                 'To'])

    if stargate_df.shape[0] > 0:
        stargate_df['functionName'] = stargate_df['functionName'].apply(lambda x: x.split('(')[0])
        stargate_df.loc[stargate_df['functionName'] == 'callBridgeCall', 'functionName'] = 'swap'

        # Add Liquidity ERC20 or ETH
        vout[['Timestamp', 'From', 'To', 'Gasused', 'Gasprice']] = stargate_df.loc[
            stargate_df['functionName'].isin(['addLiquidityETH', 'addLiquidity']), ['timeStamp_normal', 'from_normal',
                                                                                    'to_normal',
                                                                                    'gasUsed_normal', 'gasPrice']]
        vout['Kind'] = 'Stargate - Add Liquidity'

        vout['Gasused'] = [int(x) for x in vout['Gasused']]
        for i in set(vout['Timestamp']):
            vout.loc[vout['Timestamp'] == i, 'Gasused'] /= vout[vout['Timestamp'] == i].shape[0]

        stargate_df = stargate_df.loc[~stargate_df['functionName'].isin(['addLiquidityETH', 'addLiquidity'])]

        # Stake S* Token
        temp_df = stargate_df.loc[
            np.logical_and(stargate_df['functionName'] == 'deposit', stargate_df['tokenSymbol'].str.contains('\*')), [
                'timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice']].copy()
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice']
        temp_df['Kind'] = 'Stargate - Stake S* Token'

        vout = pd.concat([vout, temp_df])

        # Stake more S* with reward
        temp_df = stargate_df.loc[
            np.logical_and(stargate_df['functionName'] == 'deposit',
                           stargate_df['tokenSymbol'].str.contains('\*'))].copy()
        temp_df = pd.concat([temp_df, stargate_df[stargate_df['functionName'] == 'deposit']]).drop_duplicates(
            keep=False)
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]
        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]

        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']
        temp_df['Kind'] = 'Reward'

        stargate_df = stargate_df.loc[stargate_df['functionName'] != 'deposit']

        vout = pd.concat([vout, temp_df])

        # withdraw S* Stake with rewards
        temp_df = stargate_df.loc[stargate_df['functionName'] == 'withdraw'].copy()
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]
        temp_df.loc[temp_df['tokenSymbol'].str.contains('\*'), ['value', 'tokenSymbol']] = None
        temp_df.loc[pd.isna(temp_df['value']), 'gasUsed_normal'] = 0

        stargate_df = stargate_df.loc[stargate_df['functionName'] != 'withdraw']
        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value',
             'tokenSymbol']].copy()
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']

        if temp_df.shape[0] > 0:
            temp_df.loc[pd.isna(temp_df['To Amount']), 'Kind'] = 'Stargate - Remove S* token'

        # Withdraw veSTG
        veSTG_addresses = ['0xD4888870C8686c748232719051b677791dBDa26D'.lower()]
        temp_df.loc[temp_df['To'].isin(veSTG_addresses), ['To Coin', 'To Amount']] = None
        if temp_df.shape[0] > 0:
            temp_df.loc[~pd.isna(temp_df['To Amount']), 'Kind'] = 'Reward'

        vout = pd.concat([vout, temp_df])

        # Withdraw liquidity
        temp_df = stargate_df.loc[stargate_df['functionName'] == 'instantRedeemLocal'].copy()
        temp_df = temp_df[['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice']
        temp_df['Kind'] = 'Stargate - Remove Liquidity'
        if temp_df.shape[0] > 0:
            stargate_df = stargate_df.loc[stargate_df['functionName'] != 'instantRedeemLocal']

        vout = pd.concat([vout, temp_df])

        # Bridging ETH
        temp_df = stargate_df.loc[stargate_df['functionName'] == 'swapETH'].copy()
        temp_df['value_normal'] = [-int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value_normal']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']
        temp_df['From Coin'] = gas_coin
        temp_df['Kind'] = 'Stargate - Bridge ETH'

        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        stargate_df = stargate_df.loc[stargate_df['functionName'] != 'swapETH']

        vout = pd.concat([vout, temp_df])

        # Bridging ERC20
        temp_df = stargate_df.loc[stargate_df['functionName'].isin(['swap', ''])].copy()
        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_normal'] = [(-int(x) + int(y)) / 10 ** 18 for x, y in
                                   zip(temp_df['value_normal'], temp_df['value_internal'])]
        temp_df['value'] = [-int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]
        temp_df['gasUsed_normal'] = [str(int(int(x) / 2)) for x in temp_df['gasUsed_normal']]

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value_normal', 'value',
             'tokenSymbol']]
        temp_df1 = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value_normal']]
        temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']

        temp_df2 = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

        temp_df = pd.concat([temp_df1, temp_df2])
        if temp_df.shape[0] > 0:
            temp_df.loc[pd.isna(temp_df['From Coin']), 'From Coin'] = gas_coin
        temp_df['Kind'] = 'Stargate - Bridging'

        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        stargate_df = stargate_df.loc[~stargate_df['functionName'].isin(['swap', ''])]

        vout = pd.concat([vout, temp_df])

        # STG vesting
        temp_df = pd.DataFrame()
        temp_df[['Timestamp', 'From', 'To', 'Gasused', 'Gasprice']] = stargate_df.loc[
            stargate_df['functionName'].isin(['create_lock', 'increase_amount_and_time', 'increase_unlock_time']), [
                'timeStamp_normal', 'from_normal',
                'to_normal',
                'gasUsed_normal', 'gasPrice']]
        temp_df['Kind'] = 'Stargate - STG vesting'
        stargate_df = stargate_df.loc[
            ~stargate_df['functionName'].isin(['create_lock', 'increase_amount_and_time', 'increase_unlock_time'])]

        vout = pd.concat([vout, temp_df])

        # Claim fees
        temp_df = stargate_df.loc[stargate_df['functionName'] == 'claimTokens'].copy()
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]
        temp_df['gasUsed'] = [str(int(x) / 2) for x in temp_df['gasUsed']]

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'gasUsed_normal', 'gasPrice', 'value',
             'tokenSymbol']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']

        temp_df['Kind'] = 'Reward'

        stargate_df = stargate_df.loc[stargate_df['functionName'] != 'claimTokens']

        vout = pd.concat([vout, temp_df])

        if stargate_df.shape[0] > 0:
            print("STARGATE TRANSACTIONS ARE NOT BEING CONSIDERED")

        final_df = pd.concat(
            [final_df, vout])

        # Contract interactions Layer Zero (relayer V2)
        stargate_v2 = trx_df[trx_df['from_internal'].isin(stargate_contracts)]
        trx_df = pd.concat([trx_df, stargate_v2]).drop_duplicates(keep=False)

        stargate_v2['value'] = [int(x) / 10 ** int(y) for x, y in
                                zip(stargate_v2['value'], stargate_v2['tokenDecimal'])]
        stargate_v2['value_internal'] = [int(x) / 10 ** 18 for x in stargate_v2['value_internal']]
        stargate_v2['gasUsed_internal'] = [str(int(x) / 2) for x in stargate_v2['gasUsed_internal']]

        stargate_v2 = stargate_v2[
            ['timeStamp_internal', 'from_internal', 'to_internal', 'gasUsed_internal', 'gasPrice_erc20',
             'value_internal', 'value', 'tokenSymbol']]

        temp_df1 = stargate_v2[
            ['timeStamp_internal', 'from_internal', 'to_internal', 'gasUsed_internal', 'gasPrice_erc20',
             'value_internal']]
        temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']

        temp_df2 = stargate_v2[
            ['timeStamp_internal', 'from_internal', 'to_internal', 'gasUsed_internal', 'gasPrice_erc20', 'value',
             'tokenSymbol']]
        temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

        temp_df = pd.concat([temp_df1, temp_df2])

        if temp_df.shape[0] > 0:
            temp_df.loc[pd.isna(temp_df['From Coin']), 'From Coin'] = gas_coin

        temp_df['Kind'] = 'Stargate - Layer Zero Relayer V2'

        temp_df['Gasused'] = [int(float(x)) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        temp_df.loc[temp_df['From Amount'] > 0, 'To Amount'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Amount']
        temp_df.loc[temp_df['From Amount'] > 0, 'To Coin'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Coin']
        temp_df.loc[temp_df['From Amount'] > 0, ['From Amount', 'From Coin']] = None

        final_df = pd.concat([final_df, temp_df])

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===========================================SOFI SWAP===============================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    sofi_contracts = ['0xd55a4d54f39baf26da2f3ee7be9a6388c15f9831'.lower()]

    sofi_df = trx_df[trx_df['to_normal'].isin(sofi_contracts)].copy()
    if sofi_df.shape[0] > 0:
        trx_df = pd.concat([sofi_df, trx_df]).drop_duplicates(keep=False)

        sofi_df['functionName'] = [k.split('(')[0] for k in sofi_df['functionName']]

        # Swap with ETH
        temp_df = sofi_df[sofi_df['functionName'].str.contains('swap')]
        sofi_df = sofi_df[~sofi_df['functionName'].str.contains('swap')]

        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1
        temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
        temp_df['value_normal'] += temp_df['value_internal']

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_normal'] < 0, 'value_normal']
        temp_df.loc[temp_df['value_normal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_normal'] > 0, 'value_normal']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_normal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'Sofi Swap'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # =========================================PANCAKE SWAP=============================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    if chain == 'bsc-mainnet':
        pancake = trx_df[~pd.isna(trx_df['functionName'])].copy()
        pancake['functionName'] = pancake['functionName'].apply(lambda x: x.split('(')[0])
        pancake['to_normal'] = pancake['to_normal'].apply(lambda x: x.lower())

        syrup_pools = ['0x45c54210128a065de780C4B0Df3d16664f7f859e'.lower(),  # CAKE pool
                       '0xa80240Eb5d7E05d3F250cF000eEc0891d00b51CC'.lower()]  # Syrup Pool

        pancake.loc[np.logical_and(pancake['to_normal'].isin(syrup_pools),
                                   pancake['to'] != address), 'functionName'] = 'deposit_cake'

        # CAke and syrup pools
        pancake_dep_with = pancake.loc[pancake['functionName'].isin(['deposit_cake', 'withdrawAll'])].copy()
        harvests = pancake[pancake['functionName'] == 'harvest'].copy()

        pancake_dep_with['value'] = [int(x) / 10 ** int(y) for x, y in
                                     zip(pancake_dep_with['value'], pancake_dep_with['tokenDecimal'])]
        harvests['value'] = [int(x) / 10 ** int(y) for x, y in
                             zip(harvests['value'], harvests['tokenDecimal'])]

        hashes = list(pancake_dep_with['hash'].unique())

        pancake_dep_with.loc[pancake_dep_with['to'] != address, 'value'] *= -1

        pancake_dep_with['value'] = pancake_dep_with['value'].cumsum()
        pancake_dep_with.loc[pancake_dep_with['value'] < 0, 'value'] = None

        pancake_dep_with = pancake_dep_with[['timeStamp_normal', 'from', 'to', 'value', 'gasUsed_normal', 'gasPrice']]
        pancake_dep_with.loc[pancake_dep_with['value'] > 0, 'value'] = [
            k - harvests['value'].sum() / len(pancake_dep_with.loc[pancake_dep_with['value'] > 0, 'value']) for k in
            pancake_dep_with.loc[pancake_dep_with['value'] > 0, 'value']]

        pancake_dep_with = pd.concat(
            [pancake_dep_with, harvests[['timeStamp_normal', 'from', 'to', 'value', 'gasUsed_normal', 'gasPrice']]])
        pancake_dep_with.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice']
        pancake_dep_with['To Coin'] = 'Cake'
        pancake_dep_with['Kind'] = 'Reward'
        pancake_dep_with['From Amount'] = pancake_dep_with['From Coin'] = None
        pancake_dep_with.loc[pd.isna(pancake_dep_with['To Amount']), ['Kind', 'To Coin']] = None

        # Liquidity
        pancake_liq = pancake[pancake['functionName'].str.contains('iquidityETH')].copy()
        if pancake_liq.shape[0] > 0:
            pancake_liq['value_normal'] = [int(x) / 10 ** 18 for x in pancake_liq['value_normal']]
            pancake_liq['value_internal'] = pancake_liq['value_internal'].fillna(0)
            pancake_liq['value_internal'] = [int(x) / 10 ** 18 for x in pancake_liq['value_internal']]
            pancake_liq['value'] = [int(x) / 10 ** int(y) for x, y in
                                    zip(pancake_liq['value'], pancake_liq['tokenDecimal'])]

            hashes.extend(list(pancake_liq['hash'].unique()))

            pancake_liq.loc[pancake_liq['to'] != address, 'value'] *= -1
            pancake_liq.loc[pancake_liq['to_normal'] != address, 'value_normal'] *= -1
            pancake_liq.loc[pancake_liq['to_internal'] != address, 'value_internal'] *= -1

            pancake_liq['value_normal'] = pancake_liq['value_normal'] + pancake_liq['value_internal']
            pancake_liq = pancake_liq[pancake_liq['tokenSymbol'] != 'Cake-LP']
            pancake_liq = pancake_liq.sort_values('timeStamp')

            for token in pancake_liq.tokenSymbol.unique():
                pancake_liq.loc[pancake_liq['tokenSymbol'] == token, ['value_normal', 'value']] = pancake_liq.loc[
                    pancake_liq['tokenSymbol'] == token, ['value_normal', 'value']].cumsum()

            pancake_liq.loc[
                pancake_liq['functionName'].str.contains('add'), ['value_normal', 'value', 'tokenSymbol']] = None

            temp_df1 = pancake_liq[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value_normal']]
            temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']

            temp_df2 = pancake_liq[
                ['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
            temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

            pancake_liq = pd.concat([temp_df1, temp_df2])
            pancake_liq.loc[pd.isna(pancake_liq['From Amount']), 'Kind'] = 'Liquidity Pancake'
            pancake_liq.loc[pd.isna(pancake_liq['Kind']), 'Kind'] = 'Reward'

            pancake_liq.loc[np.logical_and(pd.isna(pancake_liq['From Coin']),
                                           pancake_liq['Kind'] == 'Reward'), 'From Coin'] = gas_coin

            pancake_liq.loc[pancake_liq['From Amount'] > 0, 'To Coin'] = pancake_liq.loc[
                pancake_liq['From Amount'] > 0, 'From Coin']
            pancake_liq.loc[pancake_liq['From Amount'] > 0, 'From Coin'] = None

            pancake_liq.loc[pancake_liq['From Amount'] > 0, 'To Amount'] = pancake_liq.loc[
                pancake_liq['From Amount'] > 0, 'From Amount']
            pancake_liq.loc[pancake_liq['From Amount'] > 0, 'From Amount'] = None

            pancake_liq['Gasused'] = [int(x) for x in pancake_liq['Gasused']]
            for i in set(pancake_liq['Timestamp']):
                pancake_liq.loc[pancake_liq['Timestamp'] == i, 'Gasused'] /= \
                    pancake_liq[pancake_liq['Timestamp'] == i].shape[0]
        else:
            pancake_liq = pd.DataFrame()

        # SWAPS
        pancake = pancake[
            pancake['functionName'].isin(
                ['swapExactTokensForTokensSupportingFeeOnTransferTokens', 'swapExactTokensForTokens',
                 'swapETHForExactTokens', 'swapExactETHForTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens',
                 'multicall'])]
        pancake['value'] = [int(x) / 10 ** int(y) for x, y in zip(pancake['value'], pancake['tokenDecimal'])]
        pancake['value_normal'] = [int(x) / 10 ** 18 for x in pancake['value_normal']]
        pancake.loc[pancake['to'] != address, 'value'] *= -1
        pancake.loc[pancake['to_normal'] != address, 'value_normal'] *= -1

        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
            pancake['value'] < 0), 'From Amount'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(
                ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
                pancake['value'] < 0), 'value']
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
            pancake['value'] > 0), 'To Amount'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(
                ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
                pancake['value'] > 0), 'value']
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
            pancake['value'] < 0), 'From Coin'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(
                ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
                pancake['value'] < 0), 'tokenSymbol']
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
            pancake['value'] > 0), 'To Coin'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(
                ['swapExactTokensForTokens', 'swapExactTokensForTokensSupportingFeeOnTransferTokens', 'multicall']),
                pancake['value'] > 0), 'tokenSymbol']

        for hash in pancake['hash'].unique():
            pancake[pancake['hash'] == hash] = pancake[pancake['hash'] == hash].ffill().bfill()

        pancake.loc[
            np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                         'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                           pancake['value'] < 0), 'From Amount'] = \
            pancake.loc[
                np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                             'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                               pancake['value'] < 0), 'value']
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactETHForTokens', 'swapETHForExactTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens']),
            pancake['value_normal'] < 0), 'From Amount'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                         'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                           pancake['value_normal'] < 0), 'value_normal']
        pancake.loc[
            np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                         'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                           pancake['value'] > 0), 'To Amount'] = \
            pancake.loc[
                np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                             'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                               pancake['value'] > 0), 'value']
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactETHForTokens', 'swapETHForExactTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens']),
            pancake['value_normal'] > 0), 'To Amount'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                         'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                           pancake['value_normal'] > 0), 'value_normal']

        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactETHForTokens', 'swapETHForExactTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens']),
            pancake['value'] < 0), 'From Coin'] = gas_coin
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactETHForTokens', 'swapETHForExactTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens']),
            pancake['value_normal'] < 0), 'From Coin'] = gas_coin
        pancake.loc[
            np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                         'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                           pancake['value'] > 0), 'To Coin'] = \
            pancake.loc[
                np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                             'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                               pancake['value'] > 0), 'tokenSymbol']
        pancake.loc[np.logical_and(pancake['functionName'].isin(
            ['swapExactETHForTokens', 'swapETHForExactTokens', 'swapExactETHForTokensSupportingFeeOnTransferTokens']),
            pancake['value_normal'] > 0), 'To Coin'] = pancake.loc[
            np.logical_and(pancake['functionName'].isin(['swapExactETHForTokens', 'swapETHForExactTokens',
                                                         'swapExactETHForTokensSupportingFeeOnTransferTokens']),
                           pancake['value_normal'] > 0), 'tokenSymbol']

        pancake['Kind'] = 'Pancake Swap Swap'
        hashes.extend(list(pancake['hash'].unique()))

        pancake = pancake[
            ['timeStamp_normal', 'from', 'to', 'To Amount', 'gasUsed_normal', 'gasPrice', 'To Coin', 'Kind',
             'From Amount', 'From Coin']].drop_duplicates(subset='timeStamp_normal')
        pancake.columns = pancake_dep_with.columns

        pancake['To'] = None
        pancake_dep_with['To'] = None

        trx_df = trx_df[~trx_df['hash'].isin(hashes)]

        final_df = pd.concat(
            [final_df, pancake, pancake_dep_with, pancake_liq])

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ============================================ONE INCH===============================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # For now only ETH <-> ERC20
    one_inch = ['0x1111111254eeb25477b68fb85ed929f73a960582'.lower(),  # ARB Router
                '0x1111111254760f7ab3f16433eea9304126dcd199'.lower()]  # Not one inch, wrapping ETH
    one_inch_df = trx_df[trx_df['from'].isin(one_inch)]
    if one_inch_df.shape[0] > 0:
        trx_df = pd.concat([trx_df, one_inch_df]).drop_duplicates(keep=False)
        one_inch_swap = one_inch_df[one_inch_df['functionName'].str.contains('swap')]
        if one_inch_swap.shape[0] != one_inch_df.shape[0]:
            print('ONE INCH PROBLEM, check code')

        one_inch_swap['value_normal'] = [int(x) / 10 ** 18 for x in one_inch_swap['value_normal']]
        one_inch_swap['value'] = [int(x) / 10 ** int(y) for x, y in
                                  zip(one_inch_swap['value'], one_inch_swap['tokenDecimal'])]

        one_inch_swap.loc[one_inch_swap['to_normal'] != address, 'value_normal'] *= -1
        one_inch_swap.loc[one_inch_swap['to'] != address, 'value'] *= -1

        one_inch_swap.loc[one_inch_swap['value'] < 0, 'From Amount'] = one_inch_swap.loc[
            one_inch_swap['value'] < 0, 'value']
        one_inch_swap.loc[one_inch_swap['value'] > 0, 'To Amount'] = one_inch_swap.loc[
            one_inch_swap['value'] > 0, 'value']

        one_inch_swap.loc[one_inch_swap['value_normal'] < 0, 'From Amount'] = one_inch_swap.loc[
            one_inch_swap['value_normal'] < 0, 'value_normal']
        one_inch_swap.loc[one_inch_swap['value_normal'] > 0, 'To Amount'] = one_inch_swap.loc[
            one_inch_swap['value_normal'] > 0, 'value_normal']

        one_inch_swap.loc[one_inch_swap['value'] < 0, 'From Coin'] = one_inch_swap.loc[
            one_inch_swap['value'] < 0, 'tokenSymbol']
        one_inch_swap.loc[one_inch_swap['value'] > 0, 'To Coin'] = one_inch_swap.loc[
            one_inch_swap['value'] > 0, 'tokenSymbol']

        one_inch_swap.loc[one_inch_swap['value_normal'] < 0, 'From Coin'] = gas_coin
        one_inch_swap.loc[one_inch_swap['value_normal'] > 0, 'To Coin'] = gas_coin

        one_inch_swap = one_inch_swap[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        one_inch_swap['Kind'] = 'One Inch swap'
        one_inch_swap.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                                 'Gasused',
                                 'Gasprice', 'Kind']
        one_inch_swap[['From', 'To']] = None
        one_inch_swap['Gasused'] = [int(x) for x in one_inch_swap['Gasused']]
        for i in set(one_inch_swap['Timestamp']):
            one_inch_swap.loc[one_inch_swap['Timestamp'] == i, 'Gasused'] /= \
                one_inch_swap[one_inch_swap['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, one_inch_swap])

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ==============================================FERRO===============================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ferro_contracts = ['0x1578c5cf4f8f6064deb167d1eead15df43185afa'.lower(),
                       '0xab50fb1117778f293cc33ac044b5579fb03029d0'.lower()]
    ferro_df = trx_df[
        np.logical_or(trx_df['from_normal'].isin(ferro_contracts), trx_df['to_normal'].isin(ferro_contracts))]

    if ferro_df.shape[0] > 0:

        trx_df = pd.concat([trx_df, ferro_df]).drop_duplicates(keep=False)
        ferro_df['functionName'] = ferro_df['functionName'].apply(lambda x: x.split('(')[0])

        # Add/Remove Liquidity tokens MISSIN WITH ETH (CRO)
        temp_df = ferro_df[ferro_df['functionName'].str.contains('Liquidity')]
        ferro_df = ferro_df[~ferro_df['functionName'].str.contains('Liquidity')]

        temp_df = temp_df[~temp_df['tokenSymbol'].str.contains('LP')]

        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

        for coin in temp_df['From Coin'].unique():
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = temp_df.loc[
                temp_df['From Coin'] == coin, 'From Amount'].cumsum()
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = \
                list(temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'])[-1]
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'From Amount'] = None
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'Kind'] = 'Uniswap/Quickswap Liquidity'

            temp_df['Gasused'] = [str(int(int(x) / 2)) for x in temp_df['Gasused']]
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'Kind'] = 'Uniswap/Quickswap Liquidity'

        temp_df['To Coin'] = None
        temp_df['To Amount'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'To Amount'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Amount']
        temp_df.loc[temp_df['From Amount'] > 0, 'To Coin'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Coin']

        temp_df.loc[temp_df['From Amount'] > 0, 'From Coin'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'From Amount'] = None

        temp_df.loc[pd.isna(temp_df['From Amount']), 'From Coin'] = None
        if temp_df.shape[0] > 0:
            temp_df.loc[pd.isna(temp_df['Kind']), 'Kind'] = 'Reward'

        final_df = pd.concat([final_df, temp_df])

        # Swap tokens
        temp_df = ferro_df[ferro_df['functionName'] == 'swap']
        ferro_df = ferro_df[ferro_df['functionName'] != 'swap']

        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'value', 'gasUsed_normal', 'gasPrice',
             'tokenSymbol']].drop_duplicates()

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']
        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'Ferro Swap'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                           'Gasused',
                           'Gasprice', 'Kind']

        for timestamp in temp_df['Timestamp'].unique():
            if temp_df.loc[temp_df['Timestamp'] == str(timestamp)].shape[0] > 1:
                temp_df.loc[temp_df['Timestamp'] == str(timestamp), ['From', 'To']] = None
                temp_df.loc[temp_df['Timestamp'] == str(timestamp)] = temp_df.loc[
                    temp_df['Timestamp'] == str(timestamp)].ffill().bfill()
        temp_df = temp_df.drop_duplicates()

        final_df = pd.concat([final_df, temp_df])

        # Deposit / Withdraw LP
        temp_df = ferro_df[ferro_df['functionName'].isin(['deposit', 'withdraw'])]
        ferro_df = ferro_df[~ferro_df['functionName'].isin(['deposit', 'withdraw'])]

        temp_df['tokenDecimal'] = temp_df['tokenDecimal'].fillna(0)
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df['tokenSymbol'] = temp_df['tokenSymbol'].fillna('')
        temp_df.loc[temp_df['tokenSymbol'].str.contains('LP'), ['tokenSymbol', 'value']] = None
        temp_df.loc[temp_df['tokenSymbol'] == 'BOOST', ['tokenSymbol', 'value']] = None

        temp_df = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']

        temp_df = temp_df.drop_duplicates()

        temp_df['Kind'] = 'Ferro Staking'
        temp_df.loc[temp_df['To Coin'] == 'FER', 'Kind'] = 'Reward'
        temp_df[['From Amount', 'From Coin']] = None

        final_df = pd.concat([final_df, temp_df])

        if ferro_df.shape[0] > 0:
            print("FERRO FINANCE TRANSACTIONS ARE NOT BEING CONSIDERED")

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===================================QUICKSWAP AND UNISWAP===========================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    uniswap_contracts = ['0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff'.lower(),
                         '0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45'.lower(),
                         '0x4C60051384bd2d3C01bfc845Cf5F4b44bcbE9de5'.lower()]
    uniswap_df = trx_df[
        np.logical_or(trx_df['from_normal'].isin(uniswap_contracts), trx_df['to_normal'].isin(uniswap_contracts))]

    if uniswap_df.shape[0] > 0:

        trx_df = pd.concat([trx_df, uniswap_df]).drop_duplicates(keep=False)
        uniswap_df['functionName'] = uniswap_df['functionName'].apply(lambda x: x.split('(')[0])

        # Liquidity with ETH
        temp_df = uniswap_df[uniswap_df['functionName'].str.contains('iquidityETH')]
        uniswap_df = uniswap_df[~uniswap_df['functionName'].str.contains('iquidityETH')]

        temp_df = temp_df[temp_df['tokenSymbol'] != 'UNI-V2']

        temp_df['value_normal'] = [(int(y) - int(x)) / 10 ** 18 for x, y in
                                   zip(temp_df['value_normal'], temp_df['value_internal'])]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df1 = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value_normal']]
        temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']

        temp_df2 = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df2.loc[temp_df2['to'] != address, 'value'] *= -1
        temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

        temp_df = pd.concat([temp_df1, temp_df2])
        temp_df['From Coin'] = temp_df['From Coin'].fillna(gas_coin)
        temp_df = temp_df.sort_values('Timestamp')

        for coin in temp_df['From Coin'].unique():
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = temp_df.loc[
                temp_df['From Coin'] == coin, 'From Amount'].cumsum()
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = \
                list(temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'])[-1]
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'From Amount'] = None
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'Kind'] = 'Uniswap/Quickswap Liquidity'

            temp_df['Gasused'] = [str(int(int(x) / 2)) for x in temp_df['Gasused']]
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'Kind'] = 'Uniswap/Quickswap Liquidity'

        temp_df['To Coin'] = None
        temp_df['To Amount'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'To Amount'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Amount']
        temp_df.loc[temp_df['From Amount'] > 0, 'To Coin'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Coin']

        temp_df.loc[temp_df['From Amount'] > 0, 'From Coin'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'From Amount'] = None

        temp_df.loc[pd.isna(temp_df['From Amount']), 'From Coin'] = None
        if temp_df.shape[0] > 0:
            temp_df.loc[pd.isna(temp_df['Kind']), 'Kind'] = 'Reward'

        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH
        temp_df = uniswap_df[uniswap_df['functionName'] == 'swapExactETHForTokens']
        uniswap_df = uniswap_df[uniswap_df['functionName'] != 'swapExactETHForTokens']

        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_normal'] < 0, 'value_normal']
        temp_df.loc[temp_df['value_normal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_normal'] > 0, 'value_normal']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_normal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'Uniswap Swap'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH via Multicall
        temp_df = uniswap_df[
            np.logical_and(uniswap_df['functionName'] == 'multicall', ~pd.isna(uniswap_df['value_internal']))]
        uniswap_df = pd.concat([uniswap_df, temp_df]).drop_duplicates(keep=False)

        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df['value_internal'] = temp_df['value_normal'] + temp_df['value_internal']
        temp_df = pd.merge(temp_df, temp_df[['hash', 'value']].groupby('hash').agg({'value': 'sum'}).reset_index(),
                           on='hash')

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'value_internal', 'value_y', 'gasUsed_normal', 'gasPrice',
             'tokenSymbol']].drop_duplicates()

        temp_df.loc[temp_df['value_y'] < 0, 'From Amount'] = temp_df.loc[temp_df['value_y'] < 0, 'value_y']
        temp_df.loc[temp_df['value_y'] > 0, 'To Amount'] = temp_df.loc[temp_df['value_y'] > 0, 'value_y']

        temp_df.loc[temp_df['value_internal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_internal'] < 0, 'value_internal']
        temp_df.loc[temp_df['value_internal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_internal'] > 0, 'value_internal']

        temp_df.loc[temp_df['value_y'] < 0, 'From Coin'] = temp_df.loc[temp_df['value_y'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value_y'] > 0, 'To Coin'] = temp_df.loc[temp_df['value_y'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_internal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_internal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'Uniswap Swap Multicall'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]
        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH via Multicall
        temp_df = uniswap_df[
            np.logical_and(uniswap_df['functionName'] == 'multicall', ~pd.isna(uniswap_df['value_internal']))]
        uniswap_df = pd.concat([uniswap_df, temp_df]).drop_duplicates(keep=False)

        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df['value_internal'] = temp_df['value_normal'] + temp_df['value_internal']
        temp_df = pd.merge(temp_df, temp_df[['hash', 'value']].groupby('hash').agg({'value': 'sum'}).reset_index(),
                           on='hash')

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'value_internal', 'value_y', 'gasUsed_normal',
             'gasPrice', 'tokenSymbol']].drop_duplicates()

        temp_df.loc[temp_df['value_y'] < 0, 'From Amount'] = temp_df.loc[temp_df['value_y'] < 0, 'value_y']
        temp_df.loc[temp_df['value_y'] > 0, 'To Amount'] = temp_df.loc[temp_df['value_y'] > 0, 'value_y']

        temp_df.loc[temp_df['value_internal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_internal'] < 0, 'value_internal']
        temp_df.loc[temp_df['value_internal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_internal'] > 0, 'value_internal']

        temp_df.loc[temp_df['value_y'] < 0, 'From Coin'] = temp_df.loc[temp_df['value_y'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value_y'] > 0, 'To Coin'] = temp_df.loc[temp_df['value_y'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_internal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_internal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'Uniswap Swap Multicall'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]
        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH via Multicall 2
        temp_df = uniswap_df[uniswap_df['functionName'] == 'multicall']
        uniswap_df = pd.concat([uniswap_df, temp_df]).drop_duplicates(keep=False)

        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'value_normal', 'value', 'gasUsed_normal',
             'gasPrice', 'tokenSymbol']].drop_duplicates()

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_normal'] < 0, 'value_normal']
        temp_df.loc[temp_df['value_normal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_normal'] > 0, 'value_normal']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_normal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'Uniswap Swap Multicall'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]
        final_df = pd.concat([final_df, temp_df])

        # Swap with EXECUTE function
        temp_df = uniswap_df[uniswap_df['functionName'] == 'execute']
        uniswap_df = pd.concat([uniswap_df, temp_df]).drop_duplicates(keep=False)

        if temp_df.shape[0] > 0:

            temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
            temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
            temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
            temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

            temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
            temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
            temp_df.loc[temp_df['to'] != address, 'value'] *= -1

            temp_df['value_internal'] = temp_df['value_normal'] + temp_df['value_internal']

            # SWAP WITH ETH EXECUTE
            temp_df1 = temp_df[temp_df['value_internal'] != 0]

            temp_df1 = temp_df1[
                ['timeStamp_normal', 'from_normal', 'to_normal', 'value_internal', 'value', 'gasUsed_normal',
                 'gasPrice',
                 'tokenSymbol']].drop_duplicates()

            temp_df1.loc[temp_df1['value'] < 0, 'From Amount'] = temp_df1.loc[temp_df1['value'] < 0, 'value']
            temp_df1.loc[temp_df1['value'] > 0, 'To Amount'] = temp_df1.loc[temp_df1['value'] > 0, 'value']

            temp_df1.loc[temp_df1['value_internal'] < 0, 'From Amount'] = temp_df1.loc[
                temp_df1['value_internal'] < 0, 'value_internal']
            temp_df1.loc[temp_df1['value_internal'] > 0, 'To Amount'] = temp_df1.loc[
                temp_df1['value_internal'] > 0, 'value_internal']

            temp_df1.loc[temp_df1['value'] < 0, 'From Coin'] = temp_df1.loc[temp_df1['value'] < 0, 'tokenSymbol']
            temp_df1.loc[temp_df1['value'] > 0, 'To Coin'] = temp_df1.loc[temp_df1['value'] > 0, 'tokenSymbol']

            temp_df1.loc[temp_df1['value_internal'] < 0, 'From Coin'] = gas_coin
            temp_df1.loc[temp_df1['value_internal'] > 0, 'To Coin'] = gas_coin

            temp_df1 = temp_df1[
                ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                 'gasUsed_normal', 'gasPrice']].copy()
            temp_df1['Kind'] = 'Uniswap Swap Execute'
            temp_df1.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                                'Gasused',
                                'Gasprice', 'Kind']
            temp_df1['Gasused'] = [int(x) for x in temp_df1['Gasused']]

            # SWAP WITH ERC20 EXECUTE
            temp_df2 = temp_df[temp_df['value_internal'] == 0]

            temp_df2 = temp_df2[
                ['timeStamp_normal', 'from_normal', 'to_normal', 'value', 'gasUsed_normal', 'gasPrice',
                 'tokenSymbol']].drop_duplicates()

            temp_df2.loc[temp_df2['value'] < 0, 'From Amount'] = temp_df2.loc[temp_df2['value'] < 0, 'value']
            temp_df2.loc[temp_df2['value'] > 0, 'To Amount'] = temp_df2.loc[temp_df2['value'] > 0, 'value']

            temp_df2.loc[temp_df2['value'] < 0, 'From Coin'] = temp_df2.loc[temp_df2['value'] < 0, 'tokenSymbol']
            temp_df2.loc[temp_df2['value'] > 0, 'To Coin'] = temp_df2.loc[temp_df2['value'] > 0, 'tokenSymbol']
            temp_df2 = temp_df2[
                ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                 'gasUsed_normal', 'gasPrice']].copy()
            temp_df2['Kind'] = 'Uniswap Swap Execute'
            temp_df2.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                                'Gasused',
                                'Gasprice', 'Kind']

            for timestamp in temp_df2['Timestamp'].unique():
                if temp_df2.loc[temp_df2['Timestamp'] == str(timestamp)].shape[0] > 1:
                    temp_df2.loc[temp_df2['Timestamp'] == str(timestamp), ['From', 'To']] = None
                    temp_df2.loc[temp_df2['Timestamp'] == str(timestamp)] = temp_df2.loc[
                        temp_df2['Timestamp'] == str(timestamp)].ffill().bfill()
            temp_df2 = temp_df2.drop_duplicates()

            final_df = pd.concat([final_df, temp_df1, temp_df2])

        if uniswap_df.shape[0] > 0:
            print("UNISWAP TRANSACTIONS ARE NOT BEING CONSIDERED")

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===============================================SANDBOX=============================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    sandbox_addresses = ['0x4AB071C42C28c4858C4BAc171F06b13586b20F30'.lower(),  # Polygon Staking contract
                         '0x214d52880b1e4E17d020908cd8EAa988FfDD4020'.lower()]  # Polygon reward distributor

    sandbox_df = trx_df[np.logical_or(trx_df['from'].isin(sandbox_addresses), trx_df['to'].isin(sandbox_addresses))]
    trx_df = pd.concat([trx_df, sandbox_df]).drop_duplicates(keep=False)

    if sandbox_df.shape[0] > 0:
        sandbox_df['functionName'] = [k.split('(')[0] if isinstance(k, str) else k for k in sandbox_df['functionName']]
        sandbox_df = pd.concat([sandbox_df, sandbox_df[np.logical_and(sandbox_df['functionName'] == 'exit', sandbox_df[
            'tokenSymbol'] != 'SAND')]]).drop_duplicates(keep=False)
        sandbox_df.loc[sandbox_df['functionName'] == 'stake', ['tokenSymbol', 'value']] = 0

        sandbox_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(sandbox_df['value'], sandbox_df['tokenDecimal'])]
        sandbox_df.loc[sandbox_df['value'] == 0, ['tokenSymbol', 'value']] = None
        sandbox_df.loc[pd.isna(sandbox_df['value']), 'Kind'] = 'Sandbox Staking'
        sandbox_df.loc[~pd.isna(sandbox_df['value']), 'Kind'] = 'Reward'
        sandbox_df = sandbox_df[
            ['timeStamp', 'from', 'to', 'gasUsed', 'gasPrice_erc20', 'value', 'tokenSymbol', 'Kind']]
        sandbox_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin', 'Kind']
        sandbox_df[['From Amount', 'From Coin']] = None

        final_df = pd.concat([final_df, sandbox_df])

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===============================================ARGO================================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    argo_contracts = ['0x84bcb1eeacc7746ad37839815548d72efb86f37f'.lower(),
                      '0xb7e2c7d79f0850aaec777f05d27c87d8c4aa32e8'.lower()]
    argo_df = trx_df[
        np.logical_or(trx_df['to_normal'].isin(argo_contracts), trx_df['from_normal'].isin(argo_contracts))]
    trx_df = pd.concat([trx_df, argo_df]).drop_duplicates(keep=False)
    if argo_df.shape[0] > 0:
        argo_df['functionName'] = argo_df['functionName'].apply(lambda x: x.split('(')[0])

        # Liquid Staking
        temp_df = argo_df[argo_df['functionName'].str.contains('stake')].copy()
        argo_df = argo_df[~argo_df['functionName'].str.contains('stake')]

        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to'] != address, 'value'] *= -1
        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
        temp_df['value_normal'] += temp_df['value_internal']

        temp_df1 = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value_normal']]
        temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']

        temp_df2 = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

        temp_df = pd.concat([temp_df1, temp_df2])
        temp_df['From Coin'] = temp_df['From Coin'].fillna(gas_coin)
        temp_df = temp_df.sort_values('Timestamp')

        temp_df['Gasused'] = [str(int(int(x) / 2)) for x in temp_df['Gasused']]
        temp_df['Kind'] = 'ARGO liquid staking'

        temp_df.loc[temp_df['From Amount'] > 0, 'To Amount'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Amount']
        temp_df.loc[temp_df['From Amount'] > 0, 'To Coin'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Coin']

        temp_df.loc[temp_df['From Amount'] > 0, 'From Coin'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'From Amount'] = None

        temp_df.loc[pd.isna(temp_df['From Amount']), 'From Coin'] = None

        for timestamp in temp_df['Timestamp'].unique():
            temp_df.loc[temp_df['Timestamp'] == timestamp, ['From Coin', 'From Amount', 'To Coin', 'To Amount']] = \
                temp_df.loc[
                    temp_df['Timestamp'] == timestamp, ['From Coin', 'From Amount', 'To Coin',
                                                        'To Amount']].ffill().bfill()

        temp_df = temp_df.drop_duplicates()
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH
        temp_df = argo_df[argo_df['functionName'].isin([''])]
        argo_df = argo_df[~argo_df['functionName'].isin([''])]

        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to'] != address, 'value'] *= -1

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()

        for timestamp in temp_df['timeStamp_normal'].unique():
            temp_df.loc[
                temp_df['timeStamp_normal'] == timestamp, ['From Coin', 'From Amount', 'To Coin', 'To Amount']] = \
                temp_df.loc[
                    temp_df['timeStamp_normal'] == timestamp, ['From Coin', 'From Amount', 'To Coin',
                                                               'To Amount']].ffill().bfill()

        temp_df['Kind'] = 'Argo Swap'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]
        temp_df = temp_df.drop_duplicates()

        final_df = pd.concat([final_df, temp_df])

        if argo_df.shape[0] > 0:
            print("ARGO TRANSACTIONS ARE NOT BEING CONSIDERED")

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===============================================MM FINANCE==========================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    mm_contracts = ['0x6be34986fdd1a91e4634eb6b9f8017439b7b5edc', '0x145677fc4d9b8f19b5d56d1820c48e0443049a30']
    mm_df = trx_df[np.logical_or(trx_df['to_normal'].isin(mm_contracts), trx_df['from_normal'].isin(mm_contracts))]
    trx_df = pd.concat([trx_df, mm_df]).drop_duplicates(keep=False)
    if mm_df.shape[0] > 0:
        mm_df['functionName'] = mm_df['functionName'].apply(lambda x: x.split('(')[0])

        # Adding/RemovingLiquidity with ETH
        temp_df = mm_df[mm_df['functionName'].str.contains('iquidityETH')]
        mm_df = mm_df[~mm_df['functionName'].str.contains('iquidityETH')]

        temp_df = temp_df[~temp_df['tokenSymbol'].str.contains('-LP')]

        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to'] != address, 'value'] *= -1
        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
        temp_df['value_normal'] += temp_df['value_internal']

        temp_df1 = temp_df[
            ['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value_normal', 'functionName']]
        temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'Function']

        temp_df2 = temp_df[
            ['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol', 'functionName']]
        temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin', 'Function']

        temp_df = pd.concat([temp_df1, temp_df2])
        temp_df['From Coin'] = temp_df['From Coin'].fillna(gas_coin)
        temp_df = temp_df.sort_values('Timestamp')

        for coin in temp_df['From Coin'].unique():
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = temp_df.loc[
                temp_df['From Coin'] == coin, 'From Amount'].cumsum()
            temp_df.loc[
                np.logical_and(temp_df['From Coin'] == coin, temp_df['Function'].str.contains('add')), ['From Amount',
                                                                                                        'From Coin']] = None
            temp_list = temp_df.loc[
                np.logical_and(temp_df['From Coin'] == coin, ~pd.isna(temp_df['From Amount'])), 'From Amount'].tolist()
            temp_list2 = [0]
            temp_list2.extend(temp_list[:-1])
            temp_df.loc[
                np.logical_and(temp_df['From Coin'] == coin, ~pd.isna(temp_df['From Amount'])), 'From Amount'] = [x - y
                                                                                                                  for
                                                                                                                  x, y
                                                                                                                  in
                                                                                                                  zip(temp_list,
                                                                                                                      temp_list2)]
            temp_df.loc[
                np.logical_and(temp_df['From Coin'] == coin, ~pd.isna(temp_df['From Amount'])), 'Kind'] = 'Reward'

        temp_df['Gasused'] = [str(int(int(x) / 2)) for x in temp_df['Gasused']]
        temp_df.loc[temp_df['Kind'] != 'Reward', 'Kind'] = 'MM Liquidity'

        temp_df.loc[temp_df['From Amount'] > 0, 'To Amount'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Amount']
        temp_df.loc[temp_df['From Amount'] > 0, 'To Coin'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Coin']

        temp_df.loc[temp_df['From Amount'] > 0, 'From Coin'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'From Amount'] = None

        temp_df.loc[pd.isna(temp_df['From Amount']), 'From Coin'] = None

        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        # Deposit / Withdraw LP
        temp_df = mm_df[mm_df['functionName'].isin(['deposit', 'withdraw'])]
        mm_df = mm_df[~mm_df['functionName'].isin(['deposit', 'withdraw'])]

        temp_df.loc[temp_df['tokenSymbol'].str.contains('-LP'), ['tokenSymbol', 'value']] = ''
        temp_df = pd.concat(
            [temp_df[np.logical_and(~temp_df['tokenName'].str.contains('LPs'), temp_df['functionName'] == 'withdraw')],
             temp_df[temp_df['functionName'] == 'deposit']])

        temp_df['value'] = [int(x) / 10 ** int(y) if x != '' else None for x, y in
                            zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']
        temp_df.loc[~pd.isna(temp_df['To Amount']), 'Kind'] = 'Reward'

        temp_df['From Coin'] = temp_df['From Coin'] = None

        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH
        temp_df = mm_df[mm_df['functionName'].isin(['swapExactETHForTokens', 'swapExactTokensForETH'])]
        mm_df = mm_df[~mm_df['functionName'].isin(['swapExactETHForTokens', 'swapExactTokensForETH'])]

        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
        temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to'] != address, 'value'] *= -1
        temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
        temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1
        temp_df['value_normal'] += temp_df['value_internal']

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_normal'] < 0, 'value_normal']
        temp_df.loc[temp_df['value_normal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_normal'] > 0, 'value_normal']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_normal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'MM Finance Swap'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        if mm_df.shape[0] > 0:
            print("MM FINANCE TRANSACTIONS ARE NOT BEING CONSIDERED")

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ========================================VVS FINANCE================================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    vvs_contracts = ['0x145863eb42cf62847a6ca784e6416c1682b1b2ae', '0xbc149c62efe8afc61728fc58b1b66a0661712e76']
    vvs_df = trx_df[np.logical_or(trx_df['to_normal'].isin(vvs_contracts), trx_df['from_normal'].isin(vvs_contracts))]
    trx_df = pd.concat([trx_df, vvs_df]).drop_duplicates(keep=False)
    if vvs_df.shape[0] > 0:
        vvs_df['functionName'] = vvs_df['functionName'].apply(lambda x: x.split('(')[0])

        # Adding/RemovingLiquidity with ETH
        temp_df = vvs_df[vvs_df['functionName'].str.contains('iquidityETH')]
        vvs_df = vvs_df[~vvs_df['functionName'].str.contains('iquidityETH')]

        temp_df = temp_df[~temp_df['tokenSymbol'].str.contains('-LP')]

        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_normal'] = [(int(y) - int(x)) / 10 ** 18 for x, y in
                                   zip(temp_df['value_normal'], temp_df['value_internal'])]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df1 = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value_normal']]
        temp_df1.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount']

        temp_df2 = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df2.loc[temp_df2['to'] != address, 'value'] *= -1
        temp_df2.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'From Amount', 'From Coin']

        temp_df = pd.concat([temp_df1, temp_df2])
        temp_df['From Coin'] = temp_df['From Coin'].fillna(gas_coin)
        temp_df = temp_df.sort_values('Timestamp')

        for coin in temp_df['From Coin'].unique():
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = temp_df.loc[
                temp_df['From Coin'] == coin, 'From Amount'].cumsum()
            temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'] = \
                list(temp_df.loc[temp_df['From Coin'] == coin, 'From Amount'])[-1]
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'From Amount'] = None
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'Kind'] = 'MMFinance Liquidity'

            temp_df['Gasused'] = [str(int(int(x) / 2)) for x in temp_df['Gasused']]
            temp_df.loc[np.logical_and(temp_df['From Coin'] == coin, temp_df['Timestamp'] != max(
                temp_df.loc[temp_df['From Coin'] == coin, 'Timestamp'])), 'Kind'] = 'VVS Finance Liquidity'

        temp_df.loc[temp_df['From Amount'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['From Amount'] > 0, 'From Amount']
        temp_df.loc[temp_df['From Amount'] > 0, 'To Coin'] = temp_df.loc[temp_df['From Amount'] > 0, 'From Coin']

        temp_df.loc[temp_df['From Amount'] > 0, 'From Coin'] = None
        temp_df.loc[temp_df['From Amount'] > 0, 'From Amount'] = None

        temp_df.loc[pd.isna(temp_df['From Amount']), 'From Coin'] = None
        if temp_df.shape[0] > 0:
            temp_df.loc[pd.isna(temp_df['Kind']), 'Kind'] = 'Reward'

        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        # Deposit / Withdraw LP
        temp_df = vvs_df[vvs_df['functionName'].isin(['deposit', 'withdraw'])]
        vvs_df = vvs_df[~vvs_df['functionName'].isin(['deposit', 'withdraw'])]

        temp_df.loc[temp_df['tokenSymbol'].str.contains('-LP'), ['tokenSymbol', 'value']] = ''
        temp_df = pd.concat([temp_df[np.logical_and(~temp_df['tokenName'].str.contains('LPs'),
                                                    temp_df['functionName'] == 'withdraw')],
                             temp_df[temp_df['functionName'] == 'deposit']])

        temp_df['value'] = [int(x) / 10 ** int(y) if x != '' else None for x, y in
                            zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df = temp_df[['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']
        temp_df.loc[~pd.isna(temp_df['To Amount']), 'Kind'] = 'Reward'

        temp_df['From Coin'] = temp_df['From Coin'] = None

        final_df = pd.concat([final_df, temp_df])

        # Swap with ETH
        temp_df = vvs_df[vvs_df['functionName'].isin(['swapExactETHForTokens', 'swapExactTokensForETH'])]
        vvs_df = vvs_df[~vvs_df['functionName'].isin(['swapExactETHForTokens', 'swapExactTokensForETH'])]

        temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
        temp_df['value_normal'] = [(int(y) - int(x)) / 10 ** 18 for x, y in
                                   zip(temp_df['value_normal'], temp_df['value_internal'])]
        temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df.loc[temp_df['to'] != address, 'value'] *= -1
        temp_df.loc[temp_df['to'] == address, 'value_normal'] *= -1

        temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
        temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Amount'] = temp_df.loc[
            temp_df['value_normal'] < 0, 'value_normal']
        temp_df.loc[temp_df['value_normal'] > 0, 'To Amount'] = temp_df.loc[
            temp_df['value_normal'] > 0, 'value_normal']

        temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
        temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

        temp_df.loc[temp_df['value_normal'] < 0, 'From Coin'] = gas_coin
        temp_df.loc[temp_df['value_normal'] > 0, 'To Coin'] = gas_coin

        temp_df = temp_df[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        temp_df['Kind'] = 'VVS Finance Swap'
        temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                           'Gasprice', 'Kind']
        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        if vvs_df.shape[0] > 0:
            print("VVS Finance TRANSACTIONS ARE NOT BEING CONSIDERED")

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===========================================MM VAULTS==============???=============================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    mmvaults_contracts = ['0xff89646fe7ee62ea96050379a7a8c532dd431d10']
    mmvaults_df = trx_df[np.logical_or(trx_df['to_normal'].isin(mmvaults_contracts),
                                       trx_df['from_normal'].isin(mmvaults_contracts))].copy()
    trx_df = pd.concat([trx_df, mmvaults_df]).drop_duplicates(keep=False)
    if mmvaults_df.shape[0] > 0:
        mmvaults_df['functionName'] = mmvaults_df['functionName'].apply(lambda x: x.split('(')[0])

        # Deposit / Withdraw LP
        temp_df = mmvaults_df[mmvaults_df['functionName'].isin(['deposit', 'withdrawAll'])]
        mmvaults_df = mmvaults_df[~mmvaults_df['functionName'].isin(['deposit', 'withdrawAll'])]

        temp_df.loc[temp_df['tokenSymbol'].str.contains('-LP'), ['tokenSymbol', 'value']] = ''
        temp_df = pd.concat([temp_df[np.logical_and(~temp_df['tokenName'].str.contains('LPs'),
                                                    temp_df['functionName'] == 'withdrawAll')],
                             temp_df[temp_df['functionName'] == 'deposit']])

        temp_df['value'] = [int(x) / 10 ** int(y) if x != '' else None for x, y in
                            zip(temp_df['value'], temp_df['tokenDecimal'])]

        temp_df = temp_df[
            ['timeStamp_normal', 'from', 'to', 'gasUsed_normal', 'gasPrice', 'value', 'tokenSymbol']]
        temp_df.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin']
        temp_df.loc[~pd.isna(temp_df['To Amount']), 'Kind'] = 'Reward'

        temp_df['From Coin'] = temp_df['From Coin'] = None

        temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
        for i in set(temp_df['Timestamp']):
            temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, temp_df])

        if mmvaults_df.shape[0] > 0:
            print("MMVaults TRANSACTIONS ARE NOT BEING CONSIDERED")

    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ===========================================0x EXCHANGE=============================================================
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    zerox_addresses = ['0xdef1abe32c034e558cdd535791643c58a13acc10'.lower()]  # optimism
    zerox_df = trx_df[trx_df['to_normal'].isin(zerox_addresses)]
    trx_df = pd.concat([zerox_df, trx_df]).drop_duplicates(keep=False)

    # Swap with ETH
    temp_df = zerox_df[zerox_df['functionName'].str.contains('transform')]
    zerox_df = zerox_df[~zerox_df['functionName'].str.contains('transform')]

    temp_df['value_internal'] = temp_df['value_internal'].fillna(0)
    temp_df['value_normal'] = [int(x) / 10 ** 18 for x in temp_df['value_normal']]
    temp_df['value_internal'] = [int(x) / 10 ** 18 for x in temp_df['value_internal']]
    temp_df['value'] = [int(x) / 10 ** int(y) for x, y in zip(temp_df['value'], temp_df['tokenDecimal'])]

    temp_df.loc[temp_df['to'] != address, 'value'] *= -1
    temp_df.loc[temp_df['to_normal'] != address, 'value_normal'] *= -1
    temp_df.loc[temp_df['to_internal'] != address, 'value_internal'] *= -1

    temp_df['value_normal'] += temp_df['value_internal']

    temp_df.loc[temp_df['value'] < 0, 'From Amount'] = temp_df.loc[temp_df['value'] < 0, 'value']
    temp_df.loc[temp_df['value'] > 0, 'To Amount'] = temp_df.loc[temp_df['value'] > 0, 'value']

    temp_df.loc[temp_df['value_normal'] < 0, 'From Amount'] = temp_df.loc[temp_df['value_normal'] < 0, 'value_normal']
    temp_df.loc[temp_df['value_normal'] > 0, 'To Amount'] = temp_df.loc[temp_df['value_normal'] > 0, 'value_normal']

    temp_df.loc[temp_df['value'] < 0, 'From Coin'] = temp_df.loc[temp_df['value'] < 0, 'tokenSymbol']
    temp_df.loc[temp_df['value'] > 0, 'To Coin'] = temp_df.loc[temp_df['value'] > 0, 'tokenSymbol']

    temp_df.loc[temp_df['value_normal'] < 0, 'From Coin'] = gas_coin
    temp_df.loc[temp_df['value_normal'] > 0, 'To Coin'] = gas_coin

    temp_df = temp_df[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
         'gasUsed_normal', 'gasPrice']].copy()
    temp_df['Kind'] = '0x Swap'
    temp_df.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                       'Gasprice', 'Kind']
    temp_df['Gasused'] = [int(x) for x in temp_df['Gasused']]
    for i in set(temp_df['Timestamp']):
        temp_df.loc[temp_df['Timestamp'] == i, 'Gasused'] /= temp_df[temp_df['Timestamp'] == i].shape[0]

    final_df = pd.concat([final_df, temp_df])

    if zerox_df.shape[0] > 0:
        print("ATTENTION: 0x TRADES NOT BEING CONSIDERED")

    # WSTEH wrapping
    wseth_hash = trx_df.loc[trx_df['tokenSymbol'] == 'wstETH', 'hash'].tolist()
    weth = trx_df[trx_df['hash'].isin(wseth_hash)].copy()
    if weth.shape[0] > 0:
        trx_df = pd.concat([trx_df, weth]).drop_duplicates(keep=False)
        weth['value'] = [int(x) / 10 ** 18 for x in weth['value']]

        weth.loc[weth['to'] != address, 'value'] *= -1

        weth = weth[
            ['timeStamp', 'from_normal', 'to_normal', 'value', 'gasUsed', 'gasPrice_erc20', 'tokenSymbol']]

        weth.loc[weth['value'] < 0, 'From Amount'] = weth.loc[weth['value'] < 0, 'value']
        weth.loc[weth['value'] > 0, 'To Amount'] = weth.loc[weth['value'] > 0, 'value']

        weth.loc[weth['value'] < 0, 'From Coin'] = weth.loc[weth['value'] < 0, 'tokenSymbol']
        weth.loc[weth['value'] > 0, 'To Coin'] = weth.loc[weth['value'] > 0, 'tokenSymbol']

        for timestamp in weth.timeStamp.unique():
            weth.loc[
                weth['timeStamp'] == timestamp, ['timeStamp', 'from_normal', 'to_normal', 'gasUsed', 'gasPrice_erc20',
                                                 'To Amount', 'To Coin', 'From Coin', 'From Amount']] = weth.loc[
                weth['timeStamp'] == timestamp, ['timeStamp', 'from_normal', 'to_normal', 'gasUsed', 'gasPrice_erc20',
                                                 'To Amount', 'To Coin', 'From Coin', 'From Amount']].ffill().bfill()

        weth = weth[
            ['timeStamp', 'from_normal', 'to_normal', 'gasUsed', 'gasPrice_erc20', 'To Amount', 'To Coin', 'From Coin',
             'From Amount']]

        weth['kind'] = f'wstETH contract'
        weth = weth.drop_duplicates()

        weth.columns = ['Timestamp', 'From', 'To', 'Gasused', 'Gasprice', 'To Amount', 'To Coin', 'From Coin',
                        'From Amount', 'Kind']

        final_df = pd.concat([final_df, weth])
        del weth
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////END SPECIFIC SMART CONTRACTS////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    # Regular ERC20 receiving
    if erc20.shape[0] > 0:
        erc20_regular_transfers = trx_df[
            np.logical_and(pd.isna(trx_df['blockHash']), ~pd.isna(trx_df['blockHash_erc20']))]
        trx_df = pd.concat([trx_df, erc20_regular_transfers]).drop_duplicates(keep=False)
        erc20_regular_transfers['value'] = [int(x) / 10 ** int(y) for x, y in
                                            zip(erc20_regular_transfers['value'],
                                                erc20_regular_transfers['tokenDecimal'])]

        erc20_regular_transfers = erc20_regular_transfers[
            ['timeStamp', 'from', 'to', 'value', 'gasUsed', 'gasPrice_erc20', 'tokenSymbol']].copy()
        erc20_regular_transfers['kind'] = 'ERC20 Transfer'
        erc20_regular_transfers.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin',
                                           'Kind']

        erc20_regular_transfers.loc[erc20_regular_transfers['To'] != address, 'To Amount'] *= -1
        erc20_regular_transfers.loc[erc20_regular_transfers['To Amount'] < 0, 'From Amount'] = \
            erc20_regular_transfers.loc[
                erc20_regular_transfers['To Amount'] < 0, 'To Amount']
        erc20_regular_transfers.loc[erc20_regular_transfers['To Amount'] < 0, 'From Coin'] = \
            erc20_regular_transfers.loc[
                erc20_regular_transfers['To Amount'] < 0, 'To Coin']
        erc20_regular_transfers.loc[erc20_regular_transfers['To Amount'] < 0, 'To Coin'] = None
        erc20_regular_transfers.loc[erc20_regular_transfers['To Amount'] < 0, 'To Amount'] = None

        for timestamp in erc20_regular_transfers['Timestamp'].unique():
            if erc20_regular_transfers.loc[erc20_regular_transfers['Timestamp'] == str(timestamp)].shape[0] > 1:
                erc20_regular_transfers.loc[
                    erc20_regular_transfers['Timestamp'] == str(timestamp), ['From', 'To']] = None

                erc20_regular_transfers.loc[erc20_regular_transfers['Timestamp'] == str(timestamp), 'Gasused'] = 0
            erc20_regular_transfers.loc[erc20_regular_transfers['Timestamp'] == str(timestamp)] = \
                erc20_regular_transfers.loc[erc20_regular_transfers['Timestamp'] == str(timestamp)].ffill().bfill()
        erc20_regular_transfers = erc20_regular_transfers.drop_duplicates()

        final_df = pd.concat(
            [final_df, erc20_regular_transfers])

    if erc721.shape[0] > 0:
        erc721_regular_transfers = trx_df[
            np.logical_and(pd.isna(trx_df['blockHash']), ~pd.isna(trx_df['blockHash_erc721']))]
        trx_df = pd.concat([trx_df, erc721_regular_transfers]).drop_duplicates(keep=False)
        erc721_regular_transfers = erc721_regular_transfers[
            ['timeStamp_erc721', 'from_erc721', 'to_erc721', 'value', 'gasUsed_erc721', 'gasPrice_erc721',
             'erc721_complete_name']]
        erc721_regular_transfers['kind'] = 'ERC721 Transfer'
        erc721_regular_transfers.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin',
                                            'Kind']

        erc721_regular_transfers.loc[erc721_regular_transfers['To'] != address, 'To Amount'] *= -1
        erc721_regular_transfers.loc[erc721_regular_transfers['To Amount'] < 0, 'From Amount'] = \
            erc721_regular_transfers.loc[
                erc721_regular_transfers['To Amount'] < 0, 'To Amount']
        erc721_regular_transfers.loc[erc721_regular_transfers['To Amount'] < 0, 'From Coin'] = \
            erc721_regular_transfers.loc[
                erc721_regular_transfers['To Amount'] < 0, 'To Coin']
        erc721_regular_transfers.loc[erc721_regular_transfers['To Amount'] < 0, 'To Coin'] = None
        erc721_regular_transfers.loc[erc721_regular_transfers['To Amount'] < 0, 'To Amount'] = None

        final_df = pd.concat([final_df, erc721_regular_transfers])

    if erc1155.shape[0] > 0:
        erc1155_regular_transfers = trx_df[
            np.logical_and(pd.isna(trx_df['blockHash']), ~pd.isna(trx_df['blockHash_erc1155']))]
        trx_df = pd.concat([trx_df, erc1155_regular_transfers]).drop_duplicates(keep=False)
        erc1155_regular_transfers = erc1155_regular_transfers[
            ['timeStamp_erc1155', 'from_erc1155', 'to_erc1155', 'tokenValue', 'gasUsed_erc1155', 'gasPrice_erc1155',
             'erc1155_complete_name']].copy()
        erc1155_regular_transfers['kind'] = 'ERC1155 Transfer'
        erc1155_regular_transfers.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin',
                                             'Kind']

        erc1155_regular_transfers.loc[erc1155_regular_transfers['To Amount'] == '', 'To Amount'] = 1
        erc1155_regular_transfers['To Amount'] = erc1155_regular_transfers['To Amount'].astype(int)

        erc1155_regular_transfers.loc[erc1155_regular_transfers['To'] != address, 'To Amount'] *= -1
        erc1155_regular_transfers.loc[erc1155_regular_transfers['To Amount'] < 0, 'From Amount'] = \
            erc1155_regular_transfers.loc[
                erc1155_regular_transfers['To Amount'] < 0, 'To Amount']
        erc1155_regular_transfers.loc[erc1155_regular_transfers['To Amount'] < 0, 'From Coin'] = \
            erc1155_regular_transfers.loc[
                erc1155_regular_transfers['To Amount'] < 0, 'To Coin']
        erc1155_regular_transfers.loc[erc1155_regular_transfers['To Amount'] < 0, 'To Coin'] = None
        erc1155_regular_transfers.loc[erc1155_regular_transfers['To Amount'] < 0, 'To Amount'] = None

        final_df = pd.concat([final_df, erc1155_regular_transfers])

    if internal.shape[0] > 0:
        internal_regular_transfers = trx_df[
            np.logical_and(pd.isna(trx_df['blockNumber_normal']), ~pd.isna(trx_df['blockNumber_internal']))]
        trx_df = pd.concat([trx_df, internal_regular_transfers]).drop_duplicates(keep=False)
        internal_regular_transfers['value_internal'] = [int(x) / 10 ** 18 for x in
                                                        internal_regular_transfers['value_internal']]
        internal_regular_transfers = internal_regular_transfers[
            ['timeStamp_internal', 'from_internal', 'to_internal', 'value_internal', 'gasUsed_internal',
             'gasPrice']].copy()
        internal_regular_transfers['tokenName'] = gas_coin
        internal_regular_transfers['kind'] = 'Internal Transfer'
        internal_regular_transfers.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin',
                                              'Kind']

        internal_regular_transfers.loc[internal_regular_transfers['To'] != address, 'To Amount'] *= -1
        internal_regular_transfers.loc[internal_regular_transfers['To Amount'] < 0, 'From Amount'] = \
            internal_regular_transfers.loc[
                internal_regular_transfers['To Amount'] < 0, 'To Amount']
        internal_regular_transfers.loc[internal_regular_transfers['To Amount'] < 0, 'From Coin'] = \
            internal_regular_transfers.loc[
                internal_regular_transfers['To Amount'] < 0, 'To Coin']
        internal_regular_transfers.loc[internal_regular_transfers['To Amount'] < 0, 'To Coin'] = None
        internal_regular_transfers.loc[internal_regular_transfers['To Amount'] < 0, 'To Amount'] = None

        final_df = pd.concat([final_df, internal_regular_transfers])

    approval_df = trx_df[trx_df['functionName'].str.contains('pprov')]
    trx_df = pd.concat([trx_df, approval_df]).drop_duplicates(keep=False)
    approval_df = approval_df[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'value_normal', 'gasUsed_normal', 'gasPrice']]
    approval_df['tokenName'] = gas_coin
    approval_df['kind'] = 'Set Approval'
    approval_df.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin', 'Kind']
    approval_df['From Coin'] = approval_df['From Amount'] = None

    # Single kind transactions
    final_df = pd.concat([final_df, approval_df])
    final_df.loc[np.logical_and(final_df['To'] == address, final_df['From'] != address), 'Gasused'] = 0

    # WETH wrapping
    weth_contracts = ['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'.lower(),
                      '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'.lower(),
                      '0x5c7f8a570d578ed84e63fdfa7b1ee72deae1ae23'.lower()]
    weth = trx_df[trx_df['to_normal'].isin(weth_contracts)].copy()
    if weth.shape[0] > 0:
        trx_df = pd.concat([trx_df, weth]).drop_duplicates(keep=False)
        weth.loc[weth['value_normal'] == '0', 'to_normal'] = weth.loc[weth['value_normal'] == '0', 'to_internal']
        weth.loc[weth['value_normal'] == '0', 'from_normal'] = weth.loc[weth['value_normal'] == '0', 'from_internal']
        weth.loc[weth['value_normal'] == '0', 'value_normal'] = weth.loc[weth['value_normal'] == '0', 'value_internal']
        weth = weth[['timeStamp_normal', 'from_normal', 'to_normal', 'value_normal', 'gasUsed_normal', 'gasPrice']]
        weth['tokenName'] = gas_coin
        weth['kind'] = f'W{gas_coin} contract'
        weth.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'To Coin', 'Kind']
        weth['To Amount'] = [int(h) for h in weth['To Amount']]

        weth.loc[weth['To'].isin(weth_contracts), 'To Coin'] = f'W{gas_coin}'
        weth.loc[weth['To'].isin(weth_contracts), 'From Coin'] = gas_coin
        weth.loc[weth['From'].isin(weth_contracts), 'To Coin'] = gas_coin
        weth.loc[weth['From'].isin(weth_contracts), 'From Coin'] = f'W{gas_coin}'

        weth['From Amount'] = weth['To Amount']
        weth['From Amount'] *= -1

        weth['To Amount'] = [int(x) / 10 ** 18 for x in weth['To Amount']]
        weth['From Amount'] = [int(x) / 10 ** 18 for x in weth['From Amount']]

        final_df = pd.concat([final_df, weth])

    # Swap between ETH <-> ERC20
    erc_swap = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber_internal']), ~pd.isna(trx_df['blockNumber']))].reset_index(
        drop=True)
    erc_swap = pd.concat([trx_df[np.logical_and(trx_df['to_normal'] == '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',
                                                trx_df['value_normal'] != '0')], erc_swap])
    trx_df = pd.concat([trx_df, erc_swap]).drop_duplicates(keep=False)

    erc_swap.loc[pd.isna(erc_swap['value_internal']), 'value_internal'] = erc_swap.loc[
        pd.isna(erc_swap['value_internal']), 'value_normal']
    erc_swap['functionName'] = [x.split('(')[0] for x in erc_swap['functionName'] if ~pd.isna(x)]

    stargate_sushi = erc_swap[erc_swap['functionName'].isin(['swapTokens', 'cook', 'instantRedeemLocal'])]
    erc_swap = erc_swap[~erc_swap['functionName'].isin(['swapTokens', 'cook', 'instantRedeemLocal'])]

    stargate_sushi['value_normal'] = [int(x) / 10 ** 18 for x in stargate_sushi['value_normal']]
    stargate_sushi['value'] = [int(x) / 10 ** int(y) for x, y in
                               zip(stargate_sushi['value'], stargate_sushi['tokenDecimal'])]

    stargate_sushi2 = stargate_sushi.copy()

    stargate_sushi2 = stargate_sushi2[
        ['timeStamp_normal', 'from', 'to', 'value', 'gasUsed_normal', 'gasPrice', 'tokenSymbol']]
    stargate_sushi = stargate_sushi[['timeStamp_normal', 'from', 'to', 'value_normal', 'gasUsed_normal', 'gasPrice']]

    stargate_sushi.columns = ['Timestamp', 'From', 'To', 'From Amount', 'Gasused', 'Gasprice']
    stargate_sushi2.columns = ['Timestamp', 'From', 'To', 'From Amount', 'Gasused', 'Gasprice', 'From Coin']

    stargate_sushi[['To Amount', 'From Coin', 'To Coin']] = None
    stargate_sushi2[['To Amount', 'To Coin']] = None
    stargate_sushi['Kind'] = stargate_sushi2['Kind'] = 'Stargate / Sushi'
    stargate_sushi['From Coin'] = gas_coin
    stargate_sushi = pd.concat([stargate_sushi, stargate_sushi2])
    stargate_sushi['From Amount'] *= -1

    stargate_sushi['Gasused'] = [int(x) for x in stargate_sushi['Gasused']]
    for i in set(stargate_sushi['Timestamp']):
        stargate_sushi.loc[stargate_sushi['Timestamp'] == i, 'Gasused'] /= \
            stargate_sushi[stargate_sushi['Timestamp'] == i].shape[0]

    erc_swap['value_internal'] = [int(x) / 10 ** 18 for x in erc_swap['value_internal']]
    erc_swap['value'] = [int(x) / 10 ** int(y) for x, y in zip(erc_swap['value'], erc_swap['tokenDecimal'])]

    erc_swap.loc[erc_swap['to_internal'] != address, 'value_internal'] *= -1
    erc_swap.loc[erc_swap['to'] != address, 'value'] *= -1

    erc_swap.loc[erc_swap['value'] < 0, 'From Amount'] = erc_swap.loc[erc_swap['value'] < 0, 'value']
    erc_swap.loc[erc_swap['value'] > 0, 'To Amount'] = erc_swap.loc[erc_swap['value'] > 0, 'value']

    erc_swap.loc[erc_swap['value_internal'] < 0, 'From Amount'] = erc_swap.loc[
        erc_swap['value_internal'] < 0, 'value_internal']
    erc_swap.loc[erc_swap['value_internal'] > 0, 'To Amount'] = erc_swap.loc[
        erc_swap['value_internal'] > 0, 'value_internal']

    erc_swap.loc[erc_swap['value'] < 0, 'From Coin'] = erc_swap.loc[erc_swap['value'] < 0, 'tokenSymbol']
    erc_swap.loc[erc_swap['value'] > 0, 'To Coin'] = erc_swap.loc[erc_swap['value'] > 0, 'tokenSymbol']

    erc_swap.loc[erc_swap['value_internal'] < 0, 'From Coin'] = gas_coin
    erc_swap.loc[erc_swap['value_internal'] > 0, 'To Coin'] = gas_coin

    erc_swap = erc_swap[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
         'gasUsed_normal', 'gasPrice']].copy()
    erc_swap['Kind'] = f'ERC {gas_coin} swap'
    erc_swap.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                        'Gasprice', 'Kind']
    erc_swap['Gasused'] = [int(x) for x in erc_swap['Gasused']]
    for i in set(erc_swap['Timestamp']):
        erc_swap.loc[erc_swap['Timestamp'] == i, 'Gasused'] /= erc_swap[erc_swap['Timestamp'] == i].shape[0]

    final_df = pd.concat([final_df, erc_swap, stargate_sushi])

    # SWAP Between ETH <-> ERC 721
    erc_swap = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber_normal']), ~pd.isna(trx_df['blockNumber_erc721']))].reset_index(
        drop=True)
    erc_swap = erc_swap[pd.isna(erc_swap['tokenSymbol'])].reset_index(drop=True)
    trx_df = pd.concat([trx_df, erc_swap]).drop_duplicates(keep=False)

    erc_swap['value_normal'] = [int(x) / 10 ** 18 for x in erc_swap['value_normal']]
    erc_swap['value'] = 1

    erc_swap.loc[erc_swap['to_normal'] != address, 'value_normal'] *= -1
    erc_swap.loc[erc_swap['to_erc721'] != address, 'value'] *= -1

    erc_swap.loc[erc_swap['value'] < 0, 'From Amount'] = erc_swap.loc[erc_swap['value'] < 0, 'value']
    erc_swap.loc[erc_swap['value'] > 0, 'To Amount'] = erc_swap.loc[erc_swap['value'] > 0, 'value']

    erc_swap.loc[erc_swap['value_normal'] < 0, 'From Amount'] = erc_swap.loc[
        erc_swap['value_normal'] < 0, 'value_normal']
    erc_swap.loc[erc_swap['value_normal'] > 0, 'To Amount'] = erc_swap.loc[erc_swap['value_normal'] > 0, 'value_normal']

    erc_swap.loc[erc_swap['value'] < 0, 'From Coin'] = erc_swap.loc[erc_swap['value'] < 0, 'erc721_complete_name']
    erc_swap.loc[erc_swap['value'] > 0, 'To Coin'] = erc_swap.loc[erc_swap['value'] > 0, 'erc721_complete_name']

    erc_swap.loc[erc_swap['value_normal'] < 0, 'From Coin'] = gas_coin
    erc_swap.loc[erc_swap['value_normal'] > 0, 'To Coin'] = gas_coin

    erc_swap = erc_swap[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
         'gasUsed_normal', 'gasPrice']].copy()
    erc_swap['Kind'] = f'ERC721 {gas_coin} swap'
    erc_swap.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                        'Gasprice', 'Kind']

    erc_swap['Gasused'] = [int(x) for x in erc_swap['Gasused']]
    for i in set(erc_swap['Timestamp']):
        erc_swap.loc[erc_swap['Timestamp'] == i, 'Gasused'] /= erc_swap[erc_swap['Timestamp'] == i].shape[0]

    final_df = pd.concat([final_df, erc_swap])

    # SWAP Between ETH <-> ERC 1155
    erc_swap = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber_normal']), ~pd.isna(trx_df['blockNumber_erc1155']))].reset_index(
        drop=True)
    erc_swap = erc_swap[pd.isna(erc_swap['tokenSymbol'])].reset_index(drop=True)
    trx_df = pd.concat([trx_df, erc_swap]).drop_duplicates(keep=False)

    erc_swap['value_normal'] = [int(x) / 10 ** 18 for x in erc_swap['value_normal']]
    erc_swap['tokenValue'] = 1

    erc_swap.loc[erc_swap['to_normal'] != address, 'value_normal'] *= -1
    erc_swap.loc[erc_swap['to_erc1155'] != address, 'tokenValue'] *= -1

    erc_swap.loc[erc_swap['tokenValue'] < 0, 'From Amount'] = erc_swap.loc[erc_swap['tokenValue'] < 0, 'tokenValue']
    erc_swap.loc[erc_swap['tokenValue'] > 0, 'To Amount'] = erc_swap.loc[erc_swap['tokenValue'] > 0, 'tokenValue']

    erc_swap.loc[erc_swap['value_normal'] < 0, 'From Amount'] = erc_swap.loc[
        erc_swap['value_normal'] < 0, 'value_normal']
    erc_swap.loc[erc_swap['value_normal'] > 0, 'To Amount'] = erc_swap.loc[erc_swap['value_normal'] > 0, 'value_normal']

    erc_swap.loc[erc_swap['tokenValue'] < 0, 'From Coin'] = erc_swap.loc[
        erc_swap['tokenValue'] < 0, 'erc1155_complete_name']
    erc_swap.loc[erc_swap['tokenValue'] > 0, 'To Coin'] = erc_swap.loc[
        erc_swap['tokenValue'] > 0, 'erc1155_complete_name']

    erc_swap.loc[erc_swap['value_normal'] < 0, 'From Coin'] = gas_coin
    erc_swap.loc[erc_swap['value_normal'] > 0, 'To Coin'] = gas_coin

    erc_swap = erc_swap[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
         'gasUsed_normal', 'gasPrice']].copy()
    erc_swap['Kind'] = f'ERC1155 {gas_coin} swap'
    erc_swap.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                        'Gasprice', 'Kind']

    erc_swap['Gasused'] = [int(x) for x in erc_swap['Gasused']]
    for i in set(erc_swap['Timestamp']):
        erc_swap.loc[erc_swap['Timestamp'] == i, 'Gasused'] /= erc_swap[erc_swap['Timestamp'] == i].shape[0]

    final_df = pd.concat([final_df, erc_swap])

    # SWAP Between ERC 20 <-> ERC 721
    erc_swap = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber']), ~pd.isna(trx_df['blockNumber_erc721']))].reset_index(
        drop=True)
    trx_df = pd.concat([trx_df, erc_swap]).drop_duplicates(keep=False)

    if erc_swap.shape[0] > 0:
        erc_swap['value_normal'] = [int(x) / 10 ** 18 for x in erc_swap['value_normal']]
        erc_swap['value'] = [int(x) / 10 ** int(y) for x, y in zip(erc_swap['value'], erc_swap['tokenDecimal'])]

        erc_swap.loc[erc_swap['to'] != address, 'value'] *= -1
        erc_swap.loc[erc_swap['to_erc721'] != address, 'value_normal'] *= -1

        erc_swap.loc[erc_swap['value_normal'] != 0, 'token'] = gas_coin
        erc_swap.loc[erc_swap['value_normal'] == 0, 'token'] = erc_swap.loc[
            erc_swap['value_normal'] == 0, 'tokenSymbol']
        erc_swap.loc[erc_swap['value_normal'] == 0, 'value_normal'] = erc_swap.loc[
            erc_swap['value_normal'] == 0, 'value']
        erc_swap.loc[erc_swap['value_normal'] == 0, 'token'] = erc_swap.loc[
            erc_swap['value_normal'] == 0, 'tokenSymbol']

        erc_swap.loc[erc_swap['value'] < 0, 'value_normal'] = -erc_swap.loc[erc_swap['value'] < 0, 'value_normal'].abs()

        erc_swap.loc[erc_swap['value_normal'] > 0, 'To Amount'] = erc_swap.loc[
            erc_swap['value_normal'] > 0, 'value_normal']
        erc_swap.loc[erc_swap['value_normal'] > 0, 'To Coin'] = erc_swap.loc[erc_swap['value_normal'] > 0, 'token']
        erc_swap.loc[erc_swap['value_normal'] < 0, 'From Amount'] = erc_swap.loc[
            erc_swap['value_normal'] < 0, 'value_normal']
        erc_swap.loc[erc_swap['value_normal'] < 0, 'From Coin'] = erc_swap.loc[erc_swap['value_normal'] < 0, 'token']

        erc_swap.loc[pd.isna(erc_swap['To Amount']), 'To Coin'] = erc_swap.loc[
            pd.isna(erc_swap['To Amount']), 'erc721_complete_name']
        erc_swap.loc[pd.isna(erc_swap['To Amount']), 'To Amount'] = 1
        erc_swap.loc[pd.isna(erc_swap['From Amount']), 'From Coin'] = erc_swap.loc[
            pd.isna(erc_swap['From Amount']), 'erc721_complete_name']
        erc_swap.loc[pd.isna(erc_swap['From Amount']), 'From Amount'] = -1

        erc_swap = erc_swap[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        erc_swap['Kind'] = 'ERC721 ERC20 swap'
        erc_swap.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                            'Gasprice', 'Kind']

        erc_swap['Gasused'] = [int(x) for x in erc_swap['Gasused']]
        for i in set(erc_swap['Timestamp']):
            erc_swap.loc[erc_swap['Timestamp'] == i, 'Gasused'] /= erc_swap[erc_swap['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, erc_swap])

    # SWAP Between ERC 20 <-> ERC1155
    erc_swap = trx_df[
        np.logical_and(~pd.isna(trx_df['blockNumber']), ~pd.isna(trx_df['blockNumber_erc1155']))].reset_index(
        drop=True)
    trx_df = pd.concat([trx_df, erc_swap]).drop_duplicates(keep=False)
    if erc_swap.shape[0] > 0:
        erc_swap['value_normal'] = [int(x) / 10 ** int(y) for x, y in zip(erc_swap['value'], erc_swap['tokenDecimal'])]
        erc_swap['tokenValue'] = 1

        erc_swap.loc[erc_swap['to'] != address, 'value_normal'] *= -1
        erc_swap.loc[erc_swap['to_erc1155'] != address, 'tokenValue'] *= -1

        erc_swap.loc[erc_swap['tokenValue'] < 0, 'From Amount'] = erc_swap.loc[erc_swap['tokenValue'] < 0, 'tokenValue']
        erc_swap.loc[erc_swap['tokenValue'] > 0, 'To Amount'] = erc_swap.loc[erc_swap['tokenValue'] > 0, 'tokenValue']

        erc_swap.loc[erc_swap['value_normal'] < 0, 'From Amount'] = erc_swap.loc[
            erc_swap['value_normal'] < 0, 'value_normal']
        erc_swap.loc[erc_swap['value_normal'] > 0, 'To Amount'] = erc_swap.loc[
            erc_swap['value_normal'] > 0, 'value_normal']

        erc_swap.loc[erc_swap['tokenValue'] < 0, 'From Coin'] = erc_swap.loc[
            erc_swap['tokenValue'] < 0, 'erc1155_complete_name']
        erc_swap.loc[erc_swap['tokenValue'] > 0, 'To Coin'] = erc_swap.loc[
            erc_swap['tokenValue'] > 0, 'erc1155_complete_name']

        erc_swap.loc[erc_swap['value_normal'] < 0, 'From Coin'] = erc_swap.loc[
            erc_swap['value_normal'] < 0, 'tokenSymbol']
        erc_swap.loc[erc_swap['value_normal'] > 0, 'To Coin'] = erc_swap.loc[
            erc_swap['value_normal'] < 0, 'tokenSymbol']

        erc_swap = erc_swap[
            ['timeStamp_normal', 'from_normal', 'to_normal', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
             'gasUsed_normal', 'gasPrice']].copy()
        erc_swap['Kind'] = f'ERC1155 {gas_coin} swap'
        erc_swap.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'Gasused',
                            'Gasprice', 'Kind']

        erc_swap['Gasused'] = [int(x) for x in erc_swap['Gasused']]
        for i in set(erc_swap['Timestamp']):
            erc_swap.loc[erc_swap['Timestamp'] == i, 'Gasused'] /= erc_swap[erc_swap['Timestamp'] == i].shape[0]

        final_df = pd.concat([final_df, erc_swap])

    # ERC20 transfers
    erc20_regular_transfers = trx_df[
        np.logical_and(~pd.isna(trx_df['tokenSymbol']), ~pd.isna(trx_df['blockNumber_normal']))].copy()
    erc20_regular_transfers = erc20_regular_transfers[
        np.logical_and(pd.isna(erc20_regular_transfers['blockNumber_erc721']),
                       pd.isna(
                           erc20_regular_transfers['erc1155_complete_name']))]
    trx_df = pd.concat([trx_df, erc20_regular_transfers]).drop_duplicates(keep=False)

    erc20_regular_transfers['value_normal'] = [int(x) / 10 ** int(y) for x, y in zip(erc20_regular_transfers['value'],
                                                                                     erc20_regular_transfers[
                                                                                         'tokenDecimal'])]

    erc20_regular_transfers.loc[erc20_regular_transfers['to'] != address, 'value_normal'] *= -1

    erc20_regular_transfers.loc[erc20_regular_transfers['value_normal'] < 0, 'From Amount'] = \
        erc20_regular_transfers.loc[
            erc20_regular_transfers['value_normal'] < 0, 'value_normal']
    erc20_regular_transfers.loc[erc20_regular_transfers['value_normal'] > 0, 'To Amount'] = erc20_regular_transfers.loc[
        erc20_regular_transfers['value_normal'] > 0, 'value_normal']

    erc20_regular_transfers.loc[erc20_regular_transfers['value_normal'] < 0, 'From Coin'] = erc20_regular_transfers.loc[
        erc20_regular_transfers['value_normal'] < 0, 'tokenSymbol']
    erc20_regular_transfers.loc[erc20_regular_transfers['value_normal'] > 0, 'To Coin'] = erc20_regular_transfers.loc[
        erc20_regular_transfers['value_normal'] > 0, 'tokenSymbol']

    erc20_regular_transfers = erc20_regular_transfers[
        ['timeStamp_normal', 'from', 'to', 'From Amount', 'To Amount', 'From Coin', 'To Coin', 'gasUsed_normal',
         'gasPrice']].copy()
    erc20_regular_transfers['Kind'] = 'ERC20 sending'

    erc20_regular_transfers.columns = ['Timestamp', 'From', 'To', 'From Amount', 'To Amount', 'From Coin', 'To Coin',
                                       'Gasused', 'Gasprice', 'Kind']

    erc20_regular_transfers['Gasused'] = [int(x) for x in erc20_regular_transfers['Gasused']]
    for i in set(erc20_regular_transfers['Timestamp']):
        erc20_regular_transfers.loc[erc20_regular_transfers['Timestamp'] == i, 'Gasused'] /= \
            erc20_regular_transfers[erc20_regular_transfers['Timestamp'] == i].shape[0]

    final_df = pd.concat([final_df, erc20_regular_transfers])

    ### Rest of transactions
    rest_df = trx_df[np.logical_and(pd.isna(trx_df['timeStamp_internal']), pd.isna(trx_df['timeStamp']))]
    error = list(rest_df.loc[rest_df['isError_normal'] == '1', 'timeStamp_normal'])
    rest_df = rest_df[np.logical_and(pd.isna(rest_df['timeStamp_erc721']), pd.isna(rest_df['timeStamp_erc1155']))]
    trx_df = pd.concat([trx_df, rest_df]).drop_duplicates(keep=False)

    if trx_df.shape[0] > 0:
        print("WARNING: Transactions are not being processed, check code")

    rest_df['kind'] = [f'{x} - {y}' for x, y in zip(rest_df['hash'], rest_df['functionName'])]
    rest_df = rest_df[
        ['timeStamp_normal', 'from_normal', 'to_normal', 'value_normal', 'gasUsed_normal', 'gasPrice', 'kind']]
    rest_df['tokenName'] = gas_coin
    rest_df.loc[rest_df['timeStamp_normal'].isin(error), 'tokenName'] = None

    rest_df.columns = ['Timestamp', 'From', 'To', 'To Amount', 'Gasused', 'Gasprice', 'Kind', 'To Coin']

    rest_df['To Amount'] = [int(x) / 10 ** 18 for x in rest_df['To Amount']]
    rest_df['From Amount'] = rest_df['From Coin'] = None

    rest_df.loc[rest_df['To'] != address, 'To Amount'] *= -1
    rest_df.loc[rest_df['To Amount'] < 0, 'From Amount'] = rest_df.loc[rest_df['To Amount'] < 0, 'To Amount']
    rest_df.loc[rest_df['To Amount'] < 0, 'From Coin'] = rest_df.loc[rest_df['To Amount'] < 0, 'To Coin']
    rest_df.loc[rest_df['To Amount'] < 0, 'To Coin'] = None
    rest_df.loc[rest_df['To Amount'] < 0, 'To Amount'] = None

    final_df = pd.concat([final_df, rest_df])

    ######################

    final_df['Gasprice'] = final_df['Gasprice'].fillna(0)
    final_df['Gasprice'] = [int(k) for k in final_df['Gasprice']]
    final_df.reset_index(drop=True, inplace=True)

    final_df['Fee'] = [-(int(x) * int(y)) / 10 ** 18 for x, y in
                       zip(list(final_df['Gasprice']), list(final_df['Gasused']))]
    final_df['Fee Coin'] = gas_coin
    final_df.index = final_df['Timestamp'].map(
        lambda x: dt.datetime.fromtimestamp(int(x))
    )

    final_df['Fee Fiat'] = None
    final_df['Fiat'] = 'EUR'
    final_df["Fiat Price"] = None

    final_df["From Amount"] = final_df["From Amount"].astype(float)
    final_df["To Amount"] = final_df["To Amount"].astype(float)

    final_df = final_df.drop(['Timestamp', 'Gasused', 'Gasprice'], axis=1)

    final_df = final_df[~final_df['To Coin'].isin(scam)]
    final_df = final_df[~final_df['From Coin'].isin(scam)]
    final_df = final_df[~final_df['From'].isin(scam)]

    final_df['To Coin'] = final_df['To Coin'].fillna('')
    final_df.loc[pd.isna(final_df['To Coin']), 'To Coin'] = ''
    final_df['From Coin'] = final_df['From Coin'].fillna('')
    final_df.loc[pd.isna(final_df['From Coin']), 'From Coin'] = ''

    final_df.loc[final_df['To Coin'].isin(nfts), 'To Amount'] = None
    final_df.loc[final_df['To Coin'].isin(nfts), 'To Coin'] = None
    final_df.loc[final_df['From Coin'].isin(nfts), 'From Amount'] = None
    final_df.loc[final_df['From Coin'].isin(nfts), 'From Coin'] = None

    final_df.loc[final_df['To Coin'].str.contains('Uniswap', na=False), 'To Amount'] = None
    final_df.loc[final_df['To Coin'].str.contains('Uniswap', na=False), 'To Coin'] = None
    final_df.loc[final_df['From Coin'].str.contains('Uniswap', na=False), 'From Amount'] = None
    final_df.loc[final_df['From Coin'].str.contains('Uniswap', na=False), 'From Coin'] = None

    final_df.loc[final_df['To Coin'].isin(['CAKE-LP', 'Cake-LP', 'UNI-V2']), 'To Amount'] = None
    final_df.loc[final_df['To Coin'].isin(['CAKE-LP', 'Cake-LP', 'UNI-V2']), 'To Coin'] = None

    final_df.loc[final_df['From Coin'].isin(['CAKE-LP', 'Cake-LP', 'UNI-V2']), 'From Amount'] = None
    final_df.loc[final_df['From Coin'].isin(['CAKE-LP', 'Cake-LP', 'UNI-V2']), 'From Coin'] = None

    final_df.loc[final_df['To Coin'] == '', 'To Coin'] = None
    final_df.loc[final_df['From Coin'] == '', 'From Coin'] = None

    final_df.loc[final_df['To Amount'] == 0, 'To Amount'] = None
    final_df.loc[final_df['From Amount'] == 0, 'From Amount'] = None

    final_df["Tag"] = final_df["Kind"]
    final_df["Source"] = f'{gas_coin}-{address[0:10]}'
    final_df["Notes"] = ''

    final_df.loc[final_df['To Coin'] == 'LOVE', 'To Amount'] = None
    final_df.loc[final_df['To Coin'] == 'LOVE', 'To Amount'] = None
    final_df.loc[final_df['To Coin'] == 'LOVE', 'To Coin'] = None

    final_df.loc[final_df['From Coin'] == 'LOVE', 'From Amount'] = None
    final_df.loc[final_df['From Coin'] == 'LOVE', 'From Coin'] = None

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

    final_df = final_df.sort_index()
    final_df.loc[final_df['From Coin'] == 'Blur Pool', 'From Amount'] = None
    final_df.loc[final_df['From Coin'] == 'Blur Pool', 'From Coin'] = None
    final_df.loc[final_df['To Coin'] == 'Blur Pool', 'To Amount'] = None
    final_df.loc[final_df['To Coin'] == 'Blur Pool', 'To Coin'] = None

    final_df.loc[final_df['From Coin'].str.contains('\*', na=False), ['From Coin', 'From Amount']] = None
    final_df.loc[final_df['To Coin'].str.contains('\*', na=False), ['To Coin', 'To Amount']] = None

    if chain == 'cro-mainnet':
        cro_org = get_crypto_dot_org_transactions(address)
        final_df = pd.concat([final_df, cro_org]).sort_index()

    eth_prices = Prices()
    final_df = tx.price_transactions_df(final_df, eth_prices)

    final_df['Fee'] = [-(abs(x)) if ~pd.isna(x) and x is not None else None for x in final_df['Fee']]
    final_df['Fee Fiat'] = [-(abs(x)) if ~pd.isna(x) and x is not None else None for x in final_df['Fee Fiat']]
    final_df['Fiat Price'] = [abs(x) if ~pd.isna(x) and x is not None else None for x in final_df['Fiat Price']]

    final_df.loc[final_df['From'] == '0x4aef1fd68c9d0b17d85e0f4e90604f6c92883f18', ['Tag', 'Notes']] = ['Reward',
                                                                                                        'Coin']

    return final_df
