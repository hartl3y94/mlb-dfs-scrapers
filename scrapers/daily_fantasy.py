import logging
from io import StringIO
from urllib.request import urlopen

import pandas as pd

from scrapers.base import BaseScraper

pd.set_option('chained_assignment', None)

class DailyFantasyScraper(BaseScraper):
    """ This class uses urllib request to parse flat data
        from url related to a single daily fantasy point
        service and format it into a csv file
    """

    def fetch(self, url, column_list, table_name):
        """ Download flat datafile from url, parse into
            dataframe and dump into s3 bucket
        """
        logging.info("Downloading %s from url", table_name)

        # GET -> memory buffer
        text = urlopen(url).read().decode('latin1')
        a = text.find('semicolons(;)</P><hr><P>') + 24
        text = text[a:]
        a = text.find('<hr><center>Statistical')
        text = text[:a]
        text = text.replace(';;;;;',';;;;')
        text = StringIO(text)

        # memory buffer -> dataframe
        df = pd.read_csv(text, sep=";", index_col=False)
        df.columns = column_list
        df['p_h'] = self.parse_pos(df['p_h'])

        # dataframe -> s3
        self.load_to_s3(df, table_name)

    @staticmethod
    def parse_pos(x):
        """ Parse the position column to hitter or pitcher """
        x[x!=1] = 'h'
        x[x==1] = 'p'
        return x
