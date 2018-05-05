"""
    This module contains functions for fetching compiled
    data from an S3 bucket
"""
from __future__ import division
import os
import logging
from io import StringIO
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
