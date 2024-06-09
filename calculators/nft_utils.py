import pandas as pd
import numpy as np
import calculators.evm_utils as eu


def opensea(df, address, columns_keep):
    df.value = df.value.infer_objects(copy=False).fillna(0)
    df.tokenDecimal = df.tokenDecimal.infer_objects(copy=False).fillna(1)
    df['value'] = eu.calculate_value_token(df.value, df.tokenDecimal)
    df.index = df['timeStamp_normal']
    df['Tag2'] = None

    vout = pd.DataFrame()
    # bulk transfers
    bulk = df[df['functionName'].str.contains('bulkTransfer', na=False)].copy()
    if bulk.shape[0] > 0:
        df = pd.concat([bulk, df]).drop_duplicates(keep=False)
        bulk = pd.merge(bulk, bulk.groupby(bulk.index).agg({'hash': 'count'}).reset_index(), left_index=True,
                        right_on='timeStamp_normal')
        bulk['Fee'] = eu.calculate_gas(bulk['gasPrice'], bulk['gasUsed_normal'])
        bulk['Fee'] /= bulk['hash_y']
        bulk['Tag'] = 'Movement'
        bulk['Notes'] = 'NFT'
        bulk = bulk.drop('hash_y', axis=1)
        bulk1155 = bulk.copy()

        bulk.loc[bulk['from_erc721'] == address, 'From Coin'] = bulk.loc[
            bulk['from_erc721'] == address, 'erc721_complete_name']
        bulk.loc[bulk['from_erc721'] == address, 'From Amount'] = -1
        bulk.loc[bulk['to_erc721'] == address, 'To Coin'] = bulk.loc[
            bulk['to_erc721'] == address, 'erc721_complete_name']
        bulk.loc[bulk['to_erc721'] == address, 'To Amount'] = 1
        bulk = bulk.drop_duplicates(subset=['From Coin', 'From Amount', 'To Coin', 'To Amount'])
        bulk['From'] = bulk['from_erc721']
        bulk['To'] = bulk['to_erc721']
        bulk = bulk[~pd.isna(bulk['to_erc721'])]

        bulk1155.loc[bulk1155['from_erc1155'] == address, 'From Coin'] = bulk1155.loc[
            bulk1155['from_erc1155'] == address, 'erc1155_complete_name']
        bulk1155.loc[bulk1155['from_erc1155'] == address, 'From Amount'] = [int(x) for x in bulk1155.loc[
            bulk1155['from_erc1155'] == address, 'tokenValue'].values]
        bulk1155.loc[bulk1155['from_erc1155'] == address, 'From Amount'] *= -1
        bulk1155.loc[bulk1155['to_erc1155'] == address, 'To Coin'] = bulk1155.loc[
            bulk1155['to_erc1155'] == address, 'erc1155_complete_name']
        bulk1155.loc[bulk1155['to_erc1155'] == address, 'To Amount'] = bulk1155.loc[
            bulk1155['to_erc1155'] == address, 'tokenValue'].values
        bulk1155 = bulk1155.drop_duplicates(subset=['From Coin', 'From Amount', 'To Coin', 'To Amount'])
        bulk1155['From'] = bulk1155['from_erc1155']
        bulk1155['To'] = bulk1155['to_erc1155']
        bulk1155 = bulk1155[~pd.isna(bulk1155['to_erc1155'])]

        vout = pd.concat([vout, bulk, bulk1155])

    # Buy and free
    buy_and_free = df[df['functionName'].str.contains('buyAndFree22457070633', na=False)].copy()
    df = pd.concat([buy_and_free, df]).drop_duplicates(keep=False)
    if buy_and_free.shape[0] > 0:
        for hash in buy_and_free.hash.unique():
            buy_and_free.loc[buy_and_free['hash'] == hash, 'value'] = buy_and_free.loc[
                buy_and_free['hash'] == hash, 'value'].sum()
        buy_and_free['Fee'] = eu.calculate_gas(buy_and_free['gasPrice_erc721'].combine_first(buy_and_free['gasPrice_erc1155']), buy_and_free['gasUsed_erc721'].combine_first(buy_and_free['gasUsed_erc1155']))

        buy_and_free['from_erc721']  =buy_and_free['from_erc721'].combine_first(buy_and_free['from_erc1155'])
        buy_and_free['to_erc721'] = buy_and_free['to_erc721'].combine_first(buy_and_free['to_erc1155'])
        buy_and_free['erc721_complete_name'] = buy_and_free['erc721_complete_name'].combine_first(buy_and_free['erc1155_complete_name'])

        buy_and_free.loc[buy_and_free['from_erc721'] == address, 'From Coin'] = buy_and_free.loc[buy_and_free['from_erc721'] == address, 'erc721_complete_name']
        buy_and_free.loc[buy_and_free['from_erc721'] == address, 'From Amount'] = 1

        # Paid with ETH instead of ERC20
        buy_and_free.loc[pd.isna(buy_and_free['tokenSymbol']), 'value'] = eu.calculate_value_eth(buy_and_free['value_normal'])
        buy_and_free.loc[pd.isna(buy_and_free['tokenSymbol']), 'tokenSymbol'] = 'ETH'


        buy_and_free.loc[buy_and_free['from_erc721'] == address, 'To Coin'] = buy_and_free.loc[buy_and_free['from_erc721'] == address, 'tokenSymbol']
        buy_and_free.loc[buy_and_free['from_erc721'] == address, 'To Amount'] = buy_and_free.loc[buy_and_free['from_erc721'] == address, 'value'].values

        buy_and_free.loc[buy_and_free['to_erc721'] == address, 'To Coin'] = buy_and_free.loc[
            buy_and_free['to_erc721'] == address, 'erc721_complete_name']

        buy_and_free.loc[buy_and_free['to_erc721'] == address, 'To Amount'] = 1
        buy_and_free.loc[buy_and_free['to_erc721'] == address, 'From Coin'] = buy_and_free.loc[
            buy_and_free['to_erc721'] == address, 'tokenSymbol']
        buy_and_free.loc[buy_and_free['to_erc721'] == address, 'From Amount'] = buy_and_free.loc[
            buy_and_free['to_erc721'] == address, 'value'].values
        buy_and_free['From Amount'] *= -1
        buy_and_free[['Tag','Notes']] = ['Trade', 'NFT']
        buy_and_free = buy_and_free.drop_duplicates(subset=columns_keep)
        vout = pd.concat([vout, buy_and_free])

    # Mints
    df.loc[df['functionName'].str.contains('mint'), 'Tag2'] = 'Mint'

    df.loc[pd.isna(df['tokenSymbol']), 'tokenSymbol'] = 'ETH'
    df['value_normal'] = eu.calculate_value_eth(df.value_normal)
    df.loc[np.logical_or(pd.isna(df['value']), df['value'] == 0), 'value'] = df.loc[
        np.logical_or(pd.isna(df['value']), df['value'] == 0), 'value_normal']

    df.loc[pd.isna(df['erc721_complete_name']), 'erc721_complete_name'] = df.loc[
        pd.isna(df['erc721_complete_name']), 'erc1155_complete_name']
    df.loc[pd.isna(df['to_erc721']), 'to_erc721'] = df.loc[
        pd.isna(df['to_erc721']), 'to_erc1155']
    df.loc[pd.isna(df['from_erc721']), 'from_erc721'] = df.loc[
        pd.isna(df['from_erc721']), 'from_erc1155']

    df['tokenValue'] = df['tokenValue'].infer_objects(copy=False).fillna(1)

    df['Fee'] = eu.calculate_gas(df.gasPrice, df.gasUsed_normal)
    df.loc[df['To'] == address, 'Fee'] = None

    df[['To', 'From']] = df[['to_erc721', 'from_erc721']]
    df.loc[df['From'] == address, 'From Coin'] = df.loc[
        df['From'] == address, 'erc721_complete_name']
    df.loc[df['To'] == address, 'To Coin'] = df.loc[
        df['To'] == address, 'erc721_complete_name']
    df.loc[df['From'] == address, 'To Coin'] = df.loc[
        df['From'] == address, 'tokenSymbol']
    df.loc[df['To'] == address, 'From Coin'] = df.loc[
        df['To'] == address, 'tokenSymbol']

    df.loc[df['From'] == address, 'From Amount'] = -df.loc[
        df['From'] == address, 'tokenValue']
    df.loc[df['To'] == address, 'To Amount'] = df.loc[df['To'] == address, 'tokenValue']
    df.loc[df['From'] == address, 'To Amount'] = df.loc[df['From'] == address, 'value']
    df.loc[df['To'] == address, 'From Amount'] = -df.loc[df['To'] == address, 'value']

    df['Notes'] = 'NFT'
    df['Tag'] = 'Trade'

    grouped = df.groupby(df.index).agg({'From Amount': 'sum', 'From': 'count', 'Fee': 'mean'}).reset_index()
    grouped.loc[grouped['From'] > 1, 'Fee'] /= grouped.loc[grouped['From'] > 1, 'From']

    grouped.index = grouped.timeStamp_normal
    grouped = grouped.drop('timeStamp_normal', axis=1)

    df['From Amount'] = grouped['From Amount']
    df['Fee'] = grouped['Fee']
    df = df.drop_duplicates()

    for x in df[df['Tag2'] == 'Mint'].index.unique():
        if df[df.index == x].shape[0] == 0:
            continue
        else:
            df.loc[df.index == x, 'From Amount'] /= df.loc[df.index == x, 'From Amount'].shape[0] * 4

    df = pd.concat([df, vout])

    df.index = df['timeStamp_normal']
    df = df[[x for x in df.columns if x in columns_keep or x == 'Tag2']]
    df = df.sort_index()
    df = df.drop('Tag2', axis=1)

    return df


def blur(df, address, columns_keep):
    df['value_normal'] = [int(x) for x in df['value_normal']]
    df['value'] = [int(x) for x in df['value'].fillna(0)]

    df.tokenDecimal = df.tokenDecimal.fillna(1)
    df['value'] = eu.calculate_value_token(df.value, df.tokenDecimal)

    df.index = df['timeStamp_normal']
    df.loc[pd.isna(df['tokenSymbol']), 'tokenSymbol'] = 'ETH'
    df['value_normal'] = eu.calculate_value_eth(df.value_normal)
    df.loc[np.logical_or(pd.isna(df['value']), df['value'] == 0), 'value'] = df.loc[
        np.logical_or(pd.isna(df['value']), df['value'] == 0), 'value_normal']

    df.loc[df['tokenSymbol'] == 'Blur Pool', 'value'] += df.loc[df['tokenSymbol'] == 'Blur Pool', 'value_normal']
    df.loc[df['tokenSymbol'] == 'Blur Pool', 'tokenSymbol'] = 'ETH'

    df.loc[pd.isna(df['erc721_complete_name']), 'erc721_complete_name'] = df.loc[
        pd.isna(df['erc721_complete_name']), 'erc1155_complete_name']
    df.loc[pd.isna(df['to_erc721']), 'to_erc721'] = df.loc[
        pd.isna(df['to_erc721']), 'to_erc1155']
    df.loc[pd.isna(df['from_erc721']), 'from_erc721'] = df.loc[
        pd.isna(df['from_erc721']), 'from_erc1155']

    df['tokenValue'] = df['tokenValue'].infer_objects(copy=False).fillna(1)

    df['Fee'] = eu.calculate_gas(df.gasPrice, df.gasUsed_normal)
    df.loc[df['To'] == address, 'Fee'] = None

    df[['To', 'From']] = df[['to_erc721', 'from_erc721']]
    df.loc[df['From'] == address, 'From Coin'] = df.loc[
        df['From'] == address, 'erc721_complete_name']
    df.loc[df['To'] == address, 'To Coin'] = df.loc[
        df['To'] == address, 'erc721_complete_name']
    df.loc[df['From'] == address, 'To Coin'] = df.loc[
        df['From'] == address, 'tokenSymbol']
    df.loc[df['To'] == address, 'From Coin'] = df.loc[
        df['To'] == address, 'tokenSymbol']

    df.loc[df['From'] == address, 'From Amount'] = -df.loc[
        df['From'] == address, 'tokenValue']
    df.loc[df['To'] == address, 'To Amount'] = df.loc[df['To'] == address, 'tokenValue']
    df.loc[df['From'] == address, 'To Amount'] = df.loc[df['From'] == address, 'value']
    df.loc[df['To'] == address, 'From Amount'] = -df.loc[df['To'] == address, 'value']

    df['Notes'] = 'NFT'
    df['Tag'] = 'Trade'

    df.loc[df['functionName'].str.contains('withdraw'), 'To Coin'] = 'ETH'
    df.loc[df['functionName'].str.contains('withdraw'), 'To Amount'] = eu.calculate_value_eth(df.loc[df['functionName'].str.contains('withdraw'), 'value_internal'])
    df.loc[df['functionName'].str.contains('withdraw'), ['Tag', 'Notes']] = ['Movement', 'Blur Pool']

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    return df


def LOTM(df, address, columns_out):
    df.index = df['timeStamp_normal']
    lotm_out = pd.DataFrame()

    df['functionName'] = [x.lower() for x in df['functionName']]

    # Claiming Vessels
    vessels_df = df[df['functionName'].str.contains('vessels')].copy()
    df = pd.concat([df, vessels_df]).drop_duplicates(keep=False)

    vessels_df['Fee'] = eu.calculate_gas(vessels_df.gasPrice, vessels_df.gasUsed_normal)
    vessels_df.loc[vessels_df['from_erc721'].apply(lambda z: z.lower()) == address, 'From Coin'] = vessels_df.loc[
        vessels_df['from_erc721'].apply(lambda z: z.lower()) == address, 'erc721_complete_name']
    vessels_df.loc[vessels_df['to_erc721'].apply(lambda z: z.lower()) == address, 'To Coin'] = vessels_df.loc[
        vessels_df['to_erc721'].apply(lambda z: z.lower()) == address, 'erc721_complete_name']

    vessels_df.loc[~pd.isna(vessels_df['From Coin']), 'From Amount'] = -1
    vessels_df.loc[~pd.isna(vessels_df['To Coin']), 'To Amount'] = 1

    vessels_df['Notes'] = 'Claim Vessels'
    vessels_df['Tag'] = 'LOTM'

    vessels_df = vessels_df[[x for x in vessels_df.columns if x in columns_out]]
    vessels_df = vessels_df.sort_index()

    grouped = vessels_df.groupby(vessels_df.index).agg({'Tag': 'count', 'Fee': 'mean'}).reset_index()
    grouped.loc[grouped['Tag'] > 1, 'Fee'] /= grouped.loc[grouped['Tag'] > 1, 'Tag']

    grouped.index = grouped.timeStamp_normal
    grouped = grouped.drop('timeStamp_normal', axis=1)

    vessels_df['Fee'] = grouped['Fee']
    vessels_df = vessels_df.drop_duplicates()
    lotm_out = pd.concat([lotm_out, vessels_df])

    # Hatching Maras
    mara_df = df[df['functionName'].str.contains('claimmaras')].copy()
    df = pd.concat([df, mara_df]).drop_duplicates(keep=False)

    mara_df['Fee'] = eu.calculate_gas(mara_df.gasPrice, mara_df.gasUsed_normal)
    mara_df.loc[mara_df['from_erc721'].apply(lambda z: z.lower()) == address, 'From Coin'] = mara_df.loc[
        mara_df['from_erc721'].apply(lambda z: z.lower()) == address, 'erc721_complete_name']
    mara_df.loc[mara_df['to_erc721'].apply(lambda z: z.lower()) == address, 'To Coin'] = mara_df.loc[
        mara_df['to_erc721'].apply(lambda z: z.lower()) == address, 'erc721_complete_name']

    mara_df.loc[~pd.isna(mara_df['From Coin']), 'From Amount'] = -1
    mara_df.loc[~pd.isna(mara_df['To Coin']), 'To Amount'] = 1

    mara_df['Notes'] = 'Claim Maras'
    mara_df['Tag'] = 'LOTM'

    mara_df = mara_df[[x for x in mara_df.columns if x in columns_out]]
    mara_df = mara_df.sort_index()

    grouped = mara_df.groupby(mara_df.index).agg({'Tag': 'count', 'Fee': 'mean'}).reset_index()
    grouped.loc[grouped['Tag'] > 1, 'Fee'] /= grouped.loc[grouped['Tag'] > 1, 'Tag']

    grouped.index = grouped.timeStamp_normal
    grouped = grouped.drop('timeStamp_normal', axis=1)

    mara_df['Fee'] = grouped['Fee']

    mara_df[['To Coin', 'To Amount']] = mara_df[['To Coin', 'To Amount']].infer_objects(copy=False).ffill()
    mara_df[['From Coin', 'From Amount']] = mara_df[['From Coin', 'From Amount']].infer_objects(copy=False).bfill()

    mara_df = mara_df.drop_duplicates()

    lotm_out = pd.concat([lotm_out, mara_df])

    # Claim ship parts or partner loot
    ship_df = df[df['functionName'].str.contains('claim')].copy()
    df = pd.concat([df, ship_df]).drop_duplicates(keep=False)

    ship_df['Fee'] = eu.calculate_gas(ship_df.gasPrice, ship_df.gasUsed_normal)
    ship_df.loc[ship_df['functionName'].str.contains('claimcatalysts'), 'erc1155_complete_name'] = ship_df.loc[
        ship_df['functionName'].str.contains('claimcatalysts'), 'erc721_complete_name']
    ship_df.loc[ship_df['functionName'].str.contains('claimcatalysts'), 'tokenValue'] = 1
    ship_df['To Coin'] = ship_df['erc1155_complete_name']
    ship_df['To Amount'] = ship_df['tokenValue']

    ship_df['Notes'] = 'Claim Ship Parts / Loot / Catalyst'
    ship_df['Tag'] = 'LOTM'

    ship_df = ship_df[[x for x in ship_df.columns if x in columns_out]]
    ship_df = ship_df.sort_index()

    grouped = ship_df.groupby(ship_df.index).agg({'Tag': 'count', 'Fee': 'mean'}).reset_index()
    grouped.loc[grouped['Tag'] > 1, 'Fee'] /= grouped.loc[grouped['Tag'] > 1, 'Tag']

    grouped.index = grouped.timeStamp_normal
    grouped = grouped.drop('timeStamp_normal', axis=1)

    ship_df['Fee'] = grouped['Fee']

    ship_df = ship_df.drop_duplicates()

    lotm_out = pd.concat([lotm_out, ship_df])

    # Kodamara Mint
    vessels_df = df[df['functionName'].str.contains('mint')].copy()
    df = pd.concat([df, vessels_df]).drop_duplicates(keep=False)

    vessels_df['Fee'] = eu.calculate_gas(vessels_df.gasPrice, vessels_df.gasUsed_normal)
    vessels_df.loc[vessels_df['from_erc721'].apply(lambda z: z.lower()) == address, 'From Coin'] = vessels_df.loc[
        vessels_df['from_erc721'].apply(lambda z: z.lower()) == address, 'erc721_complete_name']
    vessels_df.loc[vessels_df['to_erc721'].apply(lambda z: z.lower()) == address, 'To Coin'] = vessels_df.loc[
        vessels_df['to_erc721'].apply(lambda z: z.lower()) == address, 'erc721_complete_name']

    vessels_df.loc[~pd.isna(vessels_df['From Coin']), 'From Amount'] = -1
    vessels_df.loc[~pd.isna(vessels_df['To Coin']), 'To Amount'] = 1

    vessels_df['Notes'] = 'Claim Kodamara'
    vessels_df['Tag'] = 'LOTM'

    vessels_df = vessels_df[[x for x in vessels_df.columns if x in columns_out]]
    vessels_df = vessels_df.sort_index()

    grouped = vessels_df.groupby(vessels_df.index).agg({'Tag': 'count', 'Fee': 'mean'}).reset_index()
    grouped.loc[grouped['Tag'] > 1, 'Fee'] /= grouped.loc[grouped['Tag'] > 1, 'Tag']

    grouped.index = grouped.timeStamp_normal
    grouped = grouped.drop('timeStamp_normal', axis=1)

    vessels_df['Fee'] = grouped['Fee']
    vessels_df = vessels_df.drop_duplicates()
    lotm_out = pd.concat([lotm_out, vessels_df])

    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI DI LOTM SONO INCLUSE")

    return lotm_out


def the_sandbox(df, columns_out):
    df.index = df.timeStamp_normal
    df['Fee'] = eu.calculate_gas(df.gasPrice, df.gasUsed_normal)

    tsb_out = pd.DataFrame()

    tsb_1155 = df[~pd.isna(df['erc1155_complete_name'])].copy()
    df = pd.concat([df, tsb_1155]).drop_duplicates(keep=False)
    if tsb_1155.shape[0] > 0:
        tsb_1155['To Coin'] = tsb_1155['erc1155_complete_name']
        tsb_1155['To Amount'] = tsb_1155['tokenValue']

        tsb_1155['Notes'] = 'Movement'
        tsb_1155['Tag'] = 'The Sandbox - NFT'

        grouped = tsb_1155.groupby(tsb_1155.index).agg({'Tag': 'count', 'Fee': 'mean'}).reset_index()
        grouped.loc[grouped['Tag'] > 1, 'Fee'] /= grouped.loc[grouped['Tag'] > 1, 'Tag']

        grouped.index = grouped.timeStamp_normal
        grouped = grouped.drop('timeStamp_normal', axis=1)

        tsb_1155['Fee'] = grouped['Fee']

        tsb_1155 = tsb_1155.drop_duplicates()
        tsb_out = pd.concat([tsb_out, tsb_1155])

    tsb_721 = df[~pd.isna(df['erc721_complete_name'])].copy()
    df = pd.concat([df, tsb_721]).drop_duplicates(keep=False)
    if tsb_721.shape[0] > 0:
        tsb_721['To Coin'] = tsb_721['erc721_complete_name']
        tsb_721['To Amount'] = 1

        tsb_721['Notes'] = 'Movement'
        tsb_721['Tag'] = 'The Sandbox - NFT'

        grouped = tsb_721.groupby(tsb_721.index).agg({'Tag': 'count', 'Fee': 'mean'}).reset_index()
        grouped.loc[grouped['Tag'] > 1, 'Fee'] /= grouped.loc[grouped['Tag'] > 1, 'Tag']

        grouped.index = grouped.timeStamp_normal
        grouped = grouped.drop('timeStamp_normal', axis=1)

        tsb_721['Fee'] = grouped['Fee']

        tsb_721 = tsb_721.drop_duplicates()
        tsb_out = pd.concat([tsb_out, tsb_721])

    rewards = df[pd.isna(df['blockNumber_normal'])].copy()
    df = pd.concat([df, rewards]).drop_duplicates(keep=False)
    if rewards.shape[0] > 0:
        rewards['To Amount'] = eu.calculate_value_token(rewards['value'], rewards['tokenDecimal'])
        rewards[['Tag', 'Notes']] = ['Reward', 'The Sandbox - Claim']
        rewards['To Coin'] = 'SAND'
        rewards['Fee'] = eu.calculate_gas(rewards.gasPrice_erc20, rewards.gasUsed)

        tsb_out = pd.concat([tsb_out, rewards])

    staking = df[
        np.logical_or(df['functionName'].str.contains('exit'), df['functionName'].str.contains('stake'))].copy()
    df = pd.concat([df, staking]).drop_duplicates(keep=False)
    if staking.shape[0] > 0:
        staking = pd.concat([staking[staking['functionName'].str.contains('stake')],
                             staking[np.logical_and(staking['functionName'].str.contains('exit'),
                                                    staking['tokenSymbol'] != 'UNI-V2')]])

        staking['value'] = eu.calculate_value_token(staking['value'], staking['tokenDecimal'])
        staking.loc[staking['functionName'].str.contains('exit'), 'To Amount'] = staking.loc[
            staking['functionName'].str.contains('exit'), 'value']
        staking.loc[staking['functionName'].str.contains('exit'), 'To Coin'] = 'SAND'

        staking.loc[staking['functionName'].str.contains('exit'), ['Tag', 'Notes']] = ['Reward',
                                                                                       'The Sandbox - Staking']
        staking['Tag'] = staking['Tag'].fillna('Movements')
        staking['Notes'] = staking['Notes'].fillna('The Sandbox - Staking')

        tsb_out = pd.concat([tsb_out, staking])
    if df.shape[0] > 0:
        print("ATTENZIONE: NON TUTTE LE TRANSAZIONI THE SANDBOX SONO INCLUSE")

    tsb_out = tsb_out[[x for x in tsb_out.columns if x in columns_out]]
    tsb_out = tsb_out.sort_index()
    return tsb_out


def optimism_quests(df, gas_coin, columns_out):
    df.index = df['timeStamp_normal']
    df['To Coin'] = df['erc721_complete_name'].combine_first(df['erc1155_complete_name'])
    df['To Amount'] = 1
    df[['Tag', 'Notes']] = ['Trade', 'NFT']

    df['From Amount'] = eu.calculate_value_eth(df['value_normal'])
    df['From Amount'] *= -1
    df.loc[df['From Amount'] == 0, 'From Amount'] = None

    df.loc[df['From Amount'] < 0, 'From Coin'] = gas_coin

    df['Fee'] = eu.calculate_gas(df['gasPrice_erc721'].combine_first(df['gasPrice_erc1155']),
                                 df['gasUsed_erc721'].combine_first(df['gasUsed_erc1155']))

    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    return df


def decentraland_marketplace(df, columns_out):
    df.index = df['timeStamp_normal']

    create_orders = df[df['functionName'].str.contains('create')].copy()
    if create_orders.shape[0] > 0:
        df = pd.concat([df, create_orders]).drop_duplicates(keep=False)

    df['From Amount'] = eu.calculate_value_token(df['value'].fillna(0), df['tokenDecimal'].fillna(0))
    df['From Amount'] *= -1
    df['From Coin'] = 'MANA'
    df['To Coin'] = df['erc721_complete_name']
    df['To Amount'] = 1
    df['Fee'] = eu.calculate_gas(df['gasPrice_erc721'], df['gasUsed_erc721'])
    df[['Tag', 'Notes']] = ['Trade', 'NFT']
    if create_orders.shape[0] > 0:
        create_orders['Fee'] = eu.calculate_gas(create_orders['gasPrice'], create_orders['gasUsed_normal'])
        create_orders[['Tag', 'Notes']] = ['Movement', 'Decentraland - Create Order']
        df = pd.concat([df, create_orders])

    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    mana_grouped = df.groupby([df.index]).agg({'From Amount': 'sum'}).reset_index()

    df = pd.merge(mana_grouped, df, left_on='timeStamp_normal', right_index=True, suffixes=('-', ''))
    df['From Amount'] = df['From Amount-']
    df.index = df['timeStamp_normal']
    df = df.drop(['From Amount-', 'timeStamp_normal'], axis=1)
    df = df.drop_duplicates()

    return df


def nft_zksync_era(df, address, columns_out, gas_coin):
    df.index = df["timeStamp_normal"]
    df.loc[df['from_internal'] == '', 'from_internal'] = None
    df.loc[df['to_internal'] == '', 'to_internal'] = None
    df.loc[df['from_normal'] == '', 'from_normal'] = None
    df.loc[df['to_normal'] == '', 'to_normal'] = None
    df['Fee'] = eu.calculate_gas(df['gasPrice'], df['gasUsed_normal'])
    df['value'] = eu.calculate_value_token(df['value'].fillna(0), df['tokenDecimal'].fillna(0))

    df.loc[df['to'] == address, 'To Coin'] = df.loc[df['to'] == address, 'tokenSymbol']
    df.loc[df['to'] == address, 'To Amount'] = df.loc[df['to'] == address, 'value']
    df.loc[df['from'] == address, 'From Coin'] = df.loc[df['from'] == address, 'tokenSymbol']
    df.loc[df['from'] == address, 'From Amount'] = -df.loc[df['from'] == address, 'value']

    df['to_erc721'] = df['to_erc721'].apply(lambda x: x.lower())
    df['from_erc721'] = df['from_erc721'].apply(lambda x: x.lower())
    df.loc[df['from_erc721'] == df['to_erc721'], 'from_erc721'] = ''

    df.loc[df['to_erc721'] == address, 'To Coin'] = df.loc[
        df['to_erc721'] == address, 'erc721_complete_name']
    df.loc[df['to_erc721'] == address, 'To Amount'] = 1
    df.loc[df['from_erc721'] == address, 'From Coin'] = df.loc[
        df['from_erc721'] == address, 'erc721_complete_name']
    df.loc[df['from_erc721'] == address, 'From Amount'] = -1

    df['value_normal'] = eu.calculate_value_eth(df['value_normal'])
    df['value_internal'] = eu.calculate_value_eth(df['value_internal'].fillna(0))
    df['value_normal'] += df['value_internal']

    df.loc[np.logical_and(df['from_normal'] == address, pd.isna(df['From Coin'])), 'From Amount'] = - \
        df.loc[np.logical_and(df['from_normal'] == address, pd.isna(df['From Coin'])), 'value_normal']
    df.loc[np.logical_and(df['from_normal'] == address, pd.isna(df['From Coin'])), 'From Coin'] = gas_coin

    df[['Tag', 'Notes']] = ['Trade', 'NFT']

    df = df[[x for x in df.columns if x in columns_out]]
    df = df.sort_index()

    return df