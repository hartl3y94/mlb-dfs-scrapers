import os
import logging
from io import StringIO
from urllib.request import urlopen

import pandas as pd

from scrapers.base import BaseScraper


class RotoGuruScraper(BaseScraper):
    """ This class uses simple urllib request to
        download raw text from rotoguru of daily
        fantasy stats
    """
    
    def fetch(self, url, column_list, table_name):
        """ Download flat datafile directly from URL, parse
            into dataframe and dump in s3 bucket
        """
        logging.info("Downloading %s from url", table_name)

        # GET -> memory buffer
        response = urlopen(url).read().decode('latin1')
        data = StringIO(response[:response.find('\n*-ADI')])

        # memory buffer -> dataframe
        df = pd.read_csv(data, sep=":", index_col=False)
        df.columns = column_list

        # To s3
        self.load_to_s3(df, table_name)
