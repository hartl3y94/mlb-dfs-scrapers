import urllib2
import logging
from io import StringIO

import boto3
import pandas as pd

from util.config import get_config


class RotoGuruScraper(object):
    """ This class uses simple urllib2 request to
        download raw text from rotoguru of daily
        fantasy stats
    """
    pass
    