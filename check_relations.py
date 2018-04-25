import os
import logging
from io import StringIO
import boto3
import pandas as pd
import numpy as np

from util.config import get_config

def load_data():
    """ Load all data files from s3 bucket into dict """
    cfg = get_config('config.yml')

    # Get s3 bucket
    bucket = boto3.resource('s3').Bucket(cfg['s3']['bucket'])

    # Load *.csv into dataframes
    data = dict()
    for obj in bucket.objects.all():
        if '.csv' in obj.key:
            table = os.path.basename(obj.key)[:-4]
            logging.info("Loading %s", table)
            mem_buffer = StringIO(obj.get()['Body'].read().decode('latin1'))
            data[table] = pd.read_csv(mem_buffer)

    return data

def invert_name(x):
    """ Convert "Last, First" -> "First Last" """
    a = x.find(',')
    return x[a + 2:] + ' ' + x[:a]

if __name__ == '__main__':
    # Configure logging
    FORMAT = '[%(levelname)s %(asctime)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    # Get data
    data = load_data()

    # Join tables to player_link
    dfs = (
        data['dfs']
        .rename(columns={'name_first_last': 'name'})
        .merge(data['player_link'], on='mlb_id', how='left')
        .loc[lambda x: x['dk_name'].isnull()]
    )[['name', 'mlb_id']]

    fg_b = (
        data['fg_batters']
        .astype({'fg_id': str})
        .merge(data['player_link'], on='fg_id', how='left')
        .loc[lambda x: x['dk_name'].isnull()]
    )[['name', 'fg_id']]

    fg_p = (
        data['fg_pitchers']
        .astype({'fg_id': str})
        .merge(data['player_link'], on='fg_id', how='left')
        .loc[lambda x: x['dk_name'].isnull()]
    )[['name', 'fg_id']]

    sc_b = (
        data['statcast_batters']
        .merge(data['player_link'], on='mlb_id', how='left')
        .loc[lambda x: x['dk_name'].isnull()]
        .assign(name=lambda x: x['name'].apply(invert_name))
    )[['name', 'mlb_id']]

    # Combine all missing relations
    df = (
        dfs
        .merge(fg_b, on='name', how='outer')
        .merge(fg_p, on='name', how='outer')
        .merge(sc_b, on='name', how='outer')
        .drop_duplicates()
        .assign(mlb_id=lambda x: np.where(
            x['mlb_id_x'].isnull(),
            x['mlb_id_y'],
            x['mlb_id_x']
        ))
        .assign(fg_id=lambda x: np.where(
            x['fg_id_x'].isnull(),
            x['fg_id_y'],
            x['fg_id_x']
        ))
        .sort_values('name')
        .reset_index(drop=True)
        .rename(columns={'name': 'dk_name'})
    )[['dk_name', 'mlb_id', 'fg_id']]

    # Print results
    print(df)
