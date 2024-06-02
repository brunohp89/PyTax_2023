import pandas as pd
import requests
import utils

scam = ['Gods and Titans Mint Pass Original', 'Valorant Mint Pass Official', 'Walking Mushroom Mint Box Limited',
        'KRDH Official', 'Crypto Diverse Items', 'YUGA: Gucci Merch PASS', 'Overwatch SZN1 Mint Pass Limited', 'FCKIT',
        '$1000 USDC - 0', 'LIDO NFT TICKETS - 1', 'Visit ethena-2l.com to claim rewards', 'VWIN - 872448',
        'Super Boys Orignal - 1', 'TONCrypto.io', 'Warning - 31093', 'SEB', 'ZIKTOKEN.COM-SWAP AND GET 100 ZEC',
        '10 000 USD FOR FREE - 10034870321424364224204695929735558941153547521495712359208821587756539969537',
        'BEP-20 TOKEN', '$1000 REWARDS - 0', '$1000 Rewards - 0', 'BSC', 'TheSandBox.PRO', '1 stETH - 0',
        '$ELO Coin Launch Party - 0', 'FOMO', '!1 0PENSEA V0UCHER - 0', 'BSC-USD', 'MATIC BONUS - 5838', 'WGC',
        'FOMEDOG', '0xab57aef3601cad382aa499a6ae2018a69aad9cf0', 'cronosclassic.com', '$!$ 30,000 BONE - 0',
        'ApeCoin NFT - 434', '0xe592427a0aece92de3edee1f18e0157c05861564', 'LIDO WHITELIST - 1196',
        '$ Visit blasdot.com to claim rewards!', 'CyberStrife Axe Drop 1 - 234',
        '0xf3822314b333cbd7a36753b77589afbe095df1ba', 'Claim on: sandbox.droptokens.net/?claim',
        'UnisMeta 1671392971464 - 153', 'AI-A', '$USDT СLАIМ▷lootusdt.com', 'BOLG', 'KURO', '0Army.io',
        'Magma CO Pass - 28', 'OPT', '1099$ USDC - 83215', 'RTFKT - MNLTH CVD X - 199', 'ABFIN',
        'KURO  [www.kuroro.gg]', '#Synthetix.cc (sUSD)', '0x794a61358d6845594f94dc1db02a252b5b4814ad',
        'Life Goes On 846 Meta - 273', 'Airdrop on: brettbased.com', 'LOOKSDROP.COM',
        'SavageNation LW Tournament Pass Originals - 24', 'Star Fighter Club QX - 31', 'Uniswap Summer Event - 176',
        'AVALANCE NFT TICKETS - 9335', 'MNEB', 'CRONOSCLASSIC.COM', 'BAYC Airdrop - 12', 'Zepe.io', 'FCKIT',
        '2500 USDT by ETHERSCAN x METAWIN - 0', 'XEN', '5000 USDC - 0', 'TRC20CUPON.COM GET 2000usdt NOW', 'pepewife',
        'EVER', 'BlurredApeYachtClub - 0', 'APE NFT TICKETS - 8432', 'StakeEther.co', '! !$100,000 BONE - 0',
        'DarkoZoo - 9101', 'LIDO WHITELIST - 1', 'Get stETH airdrop ( https://droppradar.space )',
        'Visit https://jbonus.site to claim reward', '995$ Visit USDTReward.com to claim',
        '0x59C1A84420027e17ddb3E37e5489dc20504e9435', '0x3f0B8B206A7FBdB3ecFc08c9407CA83F5aB1Ce59',
        'Gods and Titans Mint Pass Original - 103', 'Nested Box BJ Club - 36', 'YUGA: Gucci Merch PASS - 35', 'wSNX',
        'wHEX', 'Claim on: rewards.aerodrome-network.com', '2,000 USDT Reward - 0', 'Collect on: swap-based.com',
        '$ sUSD', 'FUNGI', '5000 USDC Voucher - 0', 'Visit sufek.com and claim special rewards', 'Milady NFT Gift - 1',
        'Walking Mushroom Mint Box Limited - 509', '0xd9c242f6Ca08Fe45C58CA9E9A8558CFE7497E7c2',
        'Kodao-G Membership - 28', 'BaseWif', '$2000 USDT Airdrop - 1', 'PPBox.io',
        'Visit stETH official website:  steth-voucher.site', 'MGRT', 'DOG', 'AIR', '$$30,000 BONE - 0', 'DBT',
        'sUSD [Synthetix.cc]', 'ULand Genesis Item - 1', 'Claim at: claim.fungi-airdrop.com',
        'Acces Liquid-ether.com to claim rewards', '$1000 SPEND REWARDS - 0', 'lrETH', 'boom', 'Boom',
        'Overwatch SZN1 Mint Pass Limited - 288', 'Claim on: mogcoins.net/?claim', 'Staked Bitrock (www.bitrock.app)',
        'https://wincoin.win/', 'Visit Lido-event.site to join LIDO Giveaway!', 'Wifs', 'The QPT Originals - 336',
        'vanity-address.io', '5 ETH Voucher by Base - 0', 'wRDNT', 'PEPE WIFCOIN', '1 WETH - 560631',
        '$ Free Claim and Play', '4,651$ SHIB - 0', 'BNBW', 'BEP-20', 'BENTO', '1000$ Reward - 0',
        '0xB8c8A93168Bb610428c85EB9c9e253768C36e67D', 'The Mutants Return Land Pass Officials - 1',
        'Claim USDC/WETH at https://USDCpool.site', 'Crypto Diverse Items - 258', 'dYdX Exchange Event - 202',
        'Claim on: brettbased.com', 'BSCTOKEN', '! !$100,000 SHIB #1 - 0', 'TONCRYPTO.IO', 'BalancerV2.com',
        'BAYC Airdrop - 13', 'KRDH Official - 257', 'Acces stETHERS.COM to claim rewards',
        '$$$AIRDROP 0PENSEA - 45975694115932666297700772423167124246419737974611922237769215783254357966850', 'BS69',
        'Acces zerolends.com to claim reward', 'The Sougen Genesis Pass - 1', 'XEC', 'VERA', '$USDT СLАlМ▷airUSDT.net',
        'StarBored - 8014', 'Claim USDC at https://cusdcs.eth.li', 'DogX.AI NFT', 'Anatomic WCE Apes - 532',
        'Wiz GGY Box - 131', 'BASEMEMES', 'MKS', 'PLEB GME', 'MOEW', 'A-ZKF [www.zkfair.events]',
        'Visit blast2l.com to claim rewards', 'APE NFT TICKETS - 1781', 'SHIBPOOL.COM - 1', 'PEPEX314',
        'Valorant Mint Pass Official - 8', 'STEIN-CHESS.COM | AirDrop - You are invited', 'BLUR EVENT - 2009',
        'Otherdeed Coda Key - 2', 'ACX   [via www.across.events]', 'Claim by link: https://claim.eigen.click', 'DVP',
        'Visit 3eth.co to claim reward.', 'Visit [5000usdc.org] to claim Airdrop', 'BTN',
        'Visit rocketpool.win to claim 10 rETH.', 'ACCESS [ETHNA.CC] TO CLAIM YOUR TOKENS', '$ L-ZERO.COM | AirDrop',
        '$ Check: goldblast.io Your Gold AirDrop', 'Visit Tether.re to claim 10000 Rewards',
        'Visit Wbtc.win to claim reward', 'Visit Rocketpool.win to claim Reward.', 'Visit Shiba.pm to claim Airdrop.',
        'FRANK', 'Boe', 'BOOMER', 'BORD', 'toby', 'PEEZY', 'USA', 'WAZ', 'DOOMER', 'Nippy', 'PEPEALB'
        ]


def calculate_value_eth(values_df):
    return [int(x) / 10 ** 18 for x in values_df]


def calculate_value_token(values_df, decimals_df):
    return [int(x) / 10 ** int(y) for x, y in zip(values_df, decimals_df)]


def calculate_gas(gasprice_df, gasused_df, decimals=18):
    return [-(int(x) * int(y)) / 10 ** decimals for x, y in zip(list(gasprice_df), list(gasused_df))]


def get_transactions_raw(address, chain, scan_key=None):
    address = address.lower()
    if chain != "zksync-mainnet" and scan_key is None:
        raise ValueError(f"API key for chain {chain} is missing")

    if chain == "eth-mainnet":
        main_url = "https://api.etherscan.io/"
        gas_coin = "ETH"
    elif chain == "bsc-mainnet":
        main_url = "https://api.bscscan.com/"
        gas_coin = "BNB"
    elif chain == "arb-mainnet":
        main_url = "https://api.arbiscan.io/"
        gas_coin = "ETH"
    elif chain == "op-mainnet":
        main_url = "https://api-optimistic.etherscan.io/"
        gas_coin = "ETH"
    elif chain == "pol-mainnet":
        main_url = "https://api.polygonscan.com/"
        gas_coin = "MATIC"
    elif chain == "cro-mainnet":
        main_url = "https://api.cronoscan.com/"
        gas_coin = "CRO"
    elif chain == "base-mainnet":
        main_url = "https://api.basescan.org/"
        gas_coin = "ETH"
    elif chain == "zksync-mainnet":
        main_url = "https://block-explorer-api.mainnet.zksync.io/"
        gas_coin = "ETH"
    else:
        raise AttributeError(f"{chain} chain not recognized")

    if chain == "zksync-mainnet":
        end_block = 9007199254740991
        additional_piece = "&offset=1000"
    else:
        end_block = 9999999999999999999
        additional_piece = ""

    url = f"{main_url}api?module=account&action=txlist&address={address}&startblock=0&endblock={end_block}&sort=asc&apikey={scan_key}{additional_piece}"
    response = requests.get(url)
    normal = pd.DataFrame(response.json().get("result"))
    if normal.shape[0] == 0:
        print("No transactions found")
        return [gas_coin, pd.DataFrame(
            columns=[
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
        )]
    if normal.shape[0] == 1000 or normal.shape[0] >= 10000:  # API limits
        print("WARNING, API limit reached. Transactions are probably missing.")

    url = f"{main_url}api?module=account&action=tokennfttx&address={address}&startblock=0&endblock={end_block}&sort=asc&apikey={scan_key}{additional_piece}"
    response = requests.get(url)
    erc721 = pd.DataFrame(response.json().get("result"))
    if erc721.shape[0] > 0:
        if "tokenName" not in erc721.columns:
            erc721["erc721_complete_name"] = (
                    "ZKSYNC_NFT - " + erc721["tokenID"] + " -> " + erc721["contractAddress"]
            )
        else:
            erc721["erc721_complete_name"] = (
                    erc721["tokenName"]
                    + " - "
                    + erc721["tokenID"]
                    + " -> "
                    + erc721["contractAddress"]
            )

    erc1155 = pd.DataFrame()
    if chain not in ["arb-mainnet", "zksync-mainnet"]:
        url = f"{main_url}api?module=account&action=token1155tx&address={address}&startblock=0&endblock={end_block}&sort=asc&apikey={scan_key}{additional_piece}"
        response = requests.get(url)
        erc1155 = pd.DataFrame(response.json().get("result"))
        if erc1155.shape[0] > 0:
            erc1155["erc1155_complete_name"] = (
                    erc1155["tokenName"]
                    + " - "
                    + erc1155["tokenID"]
                    + " -> "
                    + erc1155["contractAddress"]
            )
    if chain in ["arb-mainnet", "zksync-mainnet"]:
        erc1155 = pd.DataFrame()

    url = f"{main_url}api?module=account&action=txlistinternal&address={address}&startblock=0&endblock={end_block}&sort=asc&apikey={scan_key}{additional_piece}"
    response_internal = requests.get(url)
    internal = pd.DataFrame(response_internal.json().get("result"))

    url = f"{main_url}api?module=account&action=tokentx&address={address}&startblock=0&endblock={end_block}&sort=asc&apikey={scan_key}{additional_piece}"
    response = requests.get(url)
    erc20 = pd.DataFrame(response.json().get("result"))
    if erc20.shape[0] > 0:
        erc20.loc[erc20["tokenSymbol"] == "BSC-USD", "tokenSymbol"] = "USDT"
        erc20.loc[erc20["tokenSymbol"] == "USDC.e", "tokenSymbol"] = "USDCE"
        erc20["from"] = [k.lower() for k in erc20["from"]]
        for timestamp in erc20["timeStamp"].unique():
            if (
                    list(erc20.loc[erc20["timeStamp"] == timestamp, "from"])[0]
                    == "0x4aef1fd68c9d0b17d85e0f4e90604f6c92883f18"
            ):
                erc20.loc[erc20["timeStamp"] == timestamp, "value"] = str(
                    int(list(erc20.loc[erc20["timeStamp"] == timestamp, "value"])[0])
                    * len(list(erc20.loc[erc20["timeStamp"] == timestamp, "value"]))
                )
        erc20 = erc20.drop_duplicates()
        erc20 = erc20[erc20.tokenSymbol.str.len() < 10].copy()
    if erc20.shape[0] == 1000 or erc20.shape[0] >= 10000:  # API limits
        print("WARNING, API limit reached. Transactions are probably missing.")

    if internal.shape[0] == 0:
        internal = pd.DataFrame(
            data=None,
            columns=[
                "blockNumber",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "contractAddress",
                "input",
                "type",
                "gas",
                "gasUsed",
                "traceId",
                "isError",
                "errCode",
            ],
        )
    if internal.shape[0] == 1000 or internal.shape[0] >= 10000:  # API limits
        print("WARNING, API limit reached. Transactions are probably missing.")

    if erc721.shape[0] == 0:
        erc721 = pd.DataFrame(
            data=None,
            columns=[
                "blockNumber",
                "timeStamp",
                "hash",
                "nonce",
                "blockHash",
                "from",
                "contractAddress",
                "to",
                "tokenID",
                "tokenName",
                "tokenSymbol",
                "tokenDecimal",
                "transactionIndex",
                "gas",
                "gasPrice",
                "gasUsed",
                "cumulativeGasUsed",
                "input",
                "confirmations",
                "erc721_complete_name",
            ],
        )

    if erc1155.shape[0] == 0:
        erc1155 = pd.DataFrame(
            data=None,
            columns=[
                "blockNumber",
                "timeStamp",
                "hash",
                "nonce",
                "blockHash",
                "transactionIndex",
                "gas",
                "gasPrice",
                "gasUsed",
                "cumulativeGasUsed",
                "input",
                "contractAddress",
                "from",
                "to",
                "tokenID",
                "tokenValue",
                "tokenName",
                "tokenSymbol",
                "confirmations",
                "erc1155_complete_name",
            ],
        )

    if erc20.shape[0] == 0:
        erc20 = pd.DataFrame(
            data=None,
            columns=[
                "blockNumber",
                "timeStamp",
                "hash",
                "nonce",
                "blockHash",
                "from",
                "contractAddress",
                "to",
                "value",
                "tokenName",
                "tokenSymbol",
                "tokenDecimal",
                "transactionIndex",
                "gas",
                "gasPrice",
                "gasUsed",
                "cumulativeGasUsed",
                "input",
                "confirmations",
            ],
        )

    # Scam BUSD tokens
    erc20["to"] = [k.lower() for k in erc20["to"]]
    erc20 = erc20[
        ~erc20["to"].isin(
            [
                "0x552cacece6b9448a6bc5a91cf498ddfd4b6886cc".lower(),
                "0x5525abFEB2c802C540307a29F2B5958ca9Ca86cC".lower(),
            ]
        )
    ]

    trx_df = pd.merge(
        pd.merge(
            pd.merge(
                pd.merge(
                    normal,
                    internal,
                    how="outer",
                    on="hash",
                    suffixes=("_normal", "_internal"),
                ),
                erc20,
                how="outer",
                on="hash",
                suffixes=("", "_erc20"),
            ),
            erc721,
            how="outer",
            on="hash",
            suffixes=("", "_erc721"),
        ),
        erc1155,
        how="outer",
        on="hash",
        suffixes=("", "_erc1155"),
    ).drop_duplicates()

    trx_df["from_normal"] = [
        k.lower() if not isinstance(k, float) else None for k in trx_df["from_normal"]
    ]
    trx_df["to_normal"] = [
        k.lower() if not isinstance(k, float) else None for k in trx_df["to_normal"]
    ]
    trx_df["from_internal"] = [
        k.lower() if not isinstance(k, float) else None for k in trx_df["from_internal"]
    ]
    trx_df["to_internal"] = [
        k.lower() if not isinstance(k, float) else None for k in trx_df["to_internal"]
    ]
    trx_df["to"] = [
        k.lower() if not isinstance(k, float) else None for k in trx_df["to"]
    ]
    trx_df["from"] = [
        k.lower() if not isinstance(k, float) else None for k in trx_df["from"]
    ]

    trx_df.loc[trx_df["isError_normal"] == "1", ["value_normal", "value"]] = "0"

    trx_df["timeStamp_normal"] = [utils.date_from_timestamp(k) if not isinstance(k, float) else None for k in
                                  trx_df["timeStamp_normal"]]
    trx_df["timeStamp_internal"] = [utils.date_from_timestamp(k) if not isinstance(k, float) else None for k in
                                    trx_df["timeStamp_internal"]]
    trx_df["timeStamp"] = [utils.date_from_timestamp(k) if not isinstance(k, float) else None for k in
                           trx_df["timeStamp"]]
    trx_df["timeStamp_erc721"] = [utils.date_from_timestamp(k) if not isinstance(k, float) else None for k in
                                  trx_df["timeStamp_erc721"]]
    trx_df["timeStamp_erc1155"] = [utils.date_from_timestamp(k) if not isinstance(k, float) else None for k in
                                   trx_df["timeStamp_erc1155"]]

    trx_df = trx_df[~trx_df['tokenName'].isin(scam)]
    trx_df = trx_df[~trx_df['tokenName_erc721'].isin(scam)]
    trx_df = trx_df[~trx_df['tokenName_erc1155'].isin(scam)]

    return [gas_coin, trx_df]


def eth_transfers(df, address, gas_coin, columns_keep):
    df.index = df['timeStamp_normal']

    df['value_normal'] = calculate_value_eth(df.value_normal)
    df['Fee'] = calculate_gas(df.gasPrice, df.gasUsed_normal)

    df['From'] = df['from_normal'].apply(lambda x: x.lower())
    df['To'] = df['to_normal'].apply(lambda x: x.lower())

    df['Tag'] = 'ETH transfer'

    df.loc[df['From'] == address, 'value_normal'] *= -1

    df.loc[df['To'] == address, 'Fee'] = None

    df.loc[df['value_normal'] < 0, 'From Coin'] = gas_coin
    df.loc[df['value_normal'] >= 0, 'To Coin'] = gas_coin

    df.loc[df['value_normal'] < 0, 'From Amount'] = df.loc[
        df['value_normal'] < 0, 'value_normal']
    df.loc[df['value_normal'] >= 0, 'To Amount'] = df.loc[
        df['value_normal'] >= 0, 'value_normal']

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    return df


def erc20_transfer(df, address, columns_keep):
    df.index = df['timeStamp']

    df['value_normal'] = calculate_value_token(df.value, df.tokenDecimal)
    df['gas'] = calculate_gas(df.gasPrice, df.gasUsed_normal)
    df['Fee'] = df['gas']

    df['From'] = df['from'].apply(lambda x: x.lower())
    df['To'] = df['to'].apply(lambda x: x.lower())

    df['Tag'] = 'ERC20 transfer'

    df.loc[df['From'] == address, 'value_normal'] *= -1

    df.loc[df['To'] == address, 'Fee'] = None

    df.loc[df['value_normal'] < 0, 'From Coin'] = df.loc[
        df['value_normal'] < 0, 'tokenSymbol']
    df.loc[df['value_normal'] >= 0, 'To Coin'] = df.loc[
        df['value_normal'] >= 0, 'tokenSymbol']

    df.loc[df['value_normal'] < 0, 'From Amount'] = df.loc[
        df['value_normal'] < 0, 'value_normal']
    df.loc[df['value_normal'] >= 0, 'To Amount'] = df.loc[
        df['value_normal'] >= 0, 'value_normal']

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    return df


def erc721_transfer(df, address, columns_keep):
    df.index = df['timeStamp_erc721']

    df['From'] = df['from_erc721'].apply(lambda x: x.lower())
    df['To'] = df['to_erc721'].apply(lambda x: x.lower())

    df.loc[df['To'] == address, 'To Amount'] = 1
    df.loc[df['From'] == address, 'From Amount'] = -1

    df['Fee'] = calculate_gas(df.gasPrice_erc721,
                              df.gasUsed_erc721)
    df['Tag'] = 'ERC721 transfer'

    df.loc[df['To'] == address, 'Fee'] = None

    df.loc[df['To'] == address, 'To Coin'] = df.loc[
        df['To'] == address, 'erc721_complete_name']
    df.loc[df['From'] == address, 'From Coin'] = df.loc[
        df['From'] == address, 'erc721_complete_name']

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    df['Notes'] = 'NFT'

    return df


def erc1155_transfer(df, address, columns_keep):
    df.index = df['timeStamp']

    df['Fee'] = calculate_gas(df.gasPrice_erc1155, df.gasUsed_erc1155)

    df['From'] = df['from_erc1155'].apply(lambda x: x.lower())
    df['To'] = df['to_erc1155'].apply(lambda x: x.lower())

    df['Tag'] = 'ERC1155 transfer'

    df.loc[df['From'] == address, 'tokenValue'] *= -1

    df.loc[df['To'] == address, 'Fee'] = None

    df.loc[df['value_normal'] < 0, 'From Coin'] = df.loc[
        df['value_normal'] < 0, 'tokenName_erc1155']
    df.loc[df['value_normal'] >= 0, 'To Coin'] = df.loc[
        df['value_normal'] >= 0, 'tokenName_erc1155']

    df.loc[df['value_normal'] < 0, 'From Amount'] = df.loc[
        df['value_normal'] < 0, 'tokenValue']
    df.loc[df['value_normal'] >= 0, 'To Amount'] = df.loc[
        df['value_normal'] >= 0, 'tokenValue']

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    df['Notes'] = 'NFT'

    return df


def weth(weth_df, gas_coin, columns_keep):
    if weth_df.shape[0] > 0:
        weth_df.index = weth_df['timeStamp_normal']
        weth_df[['value_normal', 'value_internal']] = weth_df[['value_normal', 'value_internal']].map(
            lambda c: 0 if pd.isna(c) else int(c))
        weth_df['value_normal'] += weth_df['value_internal']

        weth_df['value_normal'] = calculate_value_eth(weth_df['value_normal'])
        weth_df['Fee'] = calculate_gas(weth_df.gasPrice, weth_df.gasUsed_normal)

        weth_df.loc[weth_df['functionName'].str.contains('withdraw'), 'value_normal'] *= -1

        weth_df.loc[weth_df['value_normal'] < 0, 'From Amount'] = weth_df.loc[
            weth_df['value_normal'] < 0, 'value_normal']
        weth_df.loc[weth_df['value_normal'] < 0, 'To Amount'] = -weth_df.loc[
            weth_df['value_normal'] < 0, 'value_normal']
        weth_df.loc[weth_df['value_normal'] < 0, 'From Coin'] = f'W{gas_coin}'
        weth_df.loc[weth_df['value_normal'] < 0, 'To Coin'] = gas_coin

        weth_df.loc[weth_df['value_normal'] > 0, 'To Amount'] = weth_df.loc[weth_df['value_normal'] > 0, 'value_normal']
        weth_df.loc[weth_df['value_normal'] > 0, 'From Amount'] = -weth_df.loc[
            weth_df['value_normal'] > 0, 'value_normal']
        weth_df.loc[weth_df['value_normal'] > 0, 'To Coin'] = f'W{gas_coin}'
        weth_df.loc[weth_df['value_normal'] > 0, 'From Coin'] = gas_coin

        weth_df['Tag'] = 'WETH contract'

        weth_df = weth_df[[x for x in weth_df.columns if x in columns_keep]]
        weth_df = weth_df.sort_index()

        return weth_df
    else:
        return pd.DataFrame()


def ens(df, columns_keep):
    df.index = df['timeStamp_normal']
    df['Fee'] = calculate_gas(df.gasPrice, df.gasUsed_normal)
    df['value_normal'] = calculate_value_eth(df['value_normal'])

    df['From Coin'] = 'ETH'
    df['From Amount'] = -df['value_normal']
    df['To Coin'] = df['erc1155_complete_name']
    df.loc[~pd.isna(df['To Coin']), 'To Amount'] = 1

    df['Tag'] = 'ENS'
    df['Notes'] = df['functionName']

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    return df
