"""
    This module contains functions for fetching compiled
    data from an S3 bucket
"""
from __future__ import division
import os
import logging
from io import StringIO
from datetime import datetime
import boto3
import pandas as pd

from util.config import get_config

def fetch_all_csv():
    """ Fetches all .csv files from config'd s3 bucket 
        and returns in a dictionary of dataframes
    """
    cfg = get_config()

    # Target s3 bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(cfg['s3']['bucket'])

    # Loop through and append all csv files as dataframe in dict
    data = dict()
    for obj in bucket.objects.all():
        if '.csv' in obj.key:
            table = os.path.basename(obj.key)[:-4]
            logging.info(table)

            # Load string-typed data into memory buffer
            mem_buffer = StringIO(obj.get()['Body'].read().decode('latin1'))

            # Parse to dataframe
            data[table] = pd.read_csv(mem_buffer)

    return data

def write_output(df, name):
    """ Write a table to output folder in S3 bucket, indexed by date
    """
    cfg = get_config()

    # Target S3 bucket
    bucket = cfg['s3']['bucket']
    dirname = cfg['s3']['output_dir']

    # Create target filepath
    today = datetime.now().strftime("%Y%m%d")
    target_file = os.path.join(dirname, name, name + today + '.csv')

    logging.info("Loading to S3 bucket %s/%s", bucket, target_file)

    # Write file to memory buffer
    mem_buffer = StringIO()
    df.to_csv(mem_buffer, index=False)

    s3 = boto3.resource('s3')
    s3.Object(bucket, target_file).put(Body=mem_buffer.getvalue())
