import pandas as pd
import numpy as np
import calculators.evm_utils as eu


def opensea(df, address, columns_keep):
    df.value = df.value.infer_objects(copy=False).fillna(0)
    df.tokenDecimal = df.tokenDecimal.infer_objects(copy=False).fillna(1)
    df['value'] = eu.calculate_value_token(df.value, df.tokenDecimal)

    df.index = df['timeStamp_normal']
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
    df['Tag'] = 'OpenSea'

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    grouped = df.groupby(df.index).agg({'From Amount': 'sum', 'From': 'count', 'Fee': 'mean'}).reset_index()
    grouped.loc[grouped['From'] > 1, 'Fee'] /= grouped.loc[grouped['From'] > 1, 'From']

    grouped.index = grouped.timeStamp_normal
    grouped = grouped.drop('timeStamp_normal', axis=1)

    df['From Amount'] = grouped['From Amount']
    df['Fee'] = grouped['Fee']
    df = df.drop_duplicates()
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
    df['Tag'] = 'Blur'

    df = df[[x for x in df.columns if x in columns_keep]]
    df = df.sort_index()

    return df


def LOTM(df, address, columns_out):
    df.index = df['timeStamp_normal']
    lotm_out = pd.DataFrame()

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
    ship_df['To Coin'] = ship_df['erc1155_complete_name']
    ship_df['To Amount'] = ship_df['tokenValue']

    ship_df['Notes'] = 'Claim Ship Parts / Loot'
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