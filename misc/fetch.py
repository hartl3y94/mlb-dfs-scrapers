import os
import sys
import logging

path = '/home/ubuntu/mlb-dfs-scraper'


def fetch_batters():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    curdir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path)
    sys.path.append(path)

    from util import fetch

    train = fetch.get_todays_output('batters_train') 
    valid = fetch.get_todays_output('batters_valid')

    os.chdir(curdir)
    return train, valid

