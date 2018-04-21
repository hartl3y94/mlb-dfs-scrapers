import logging
from urllib.request import urlopen
from datetime import datetime as dt

import pandas as pd

from scrapers.base import BaseScraper

class VegasScraper(BaseScraper):
    """ This class downloads current Vegas lines from
        json-like javascript data source
    """

    def fetch(self, url, table_name):
        """ Download webpage source and parse into
            simple dataframe format
        """
        logging.info("Downloading %s from %s", table_name, url)

        # url -> string
        body = urlopen(url).read().decode('latin1')

        # string -> dataframe
        df = self.parse_js(body)

        # -> s3
        self.load_to_s3(df, table_name)

    #######################################################
    # All of the below methods were written a long time ago
    # many code iterations ago. They are very tedious but they
    # work so I have no motivation to clean them up and rewrite
    #######################################################

    @staticmethod
    def split(txt, sub):
        """ Split text into two strings based on substring """
        a = txt.find(sub)
        return txt[:a].strip(), txt[a+len(sub):]

    def parse_js(self, body, df=pd.DataFrame()):
        """ Recursively parse rows of JSON data to proper
            pandas dataframe format
        """
        if body.find('time') == -1:
            return df
        _, body = self.split(body, '"team":"')
        team, body = self.split(body, '","')
        _, body = self.split(body, 'opponent":')
        _, body = self.split(body, ' ')
        oppo, body = self.split(body, '","')
        _, body = self.split(body, 'line:":"')
        line, body = self.split(body, '","')
        _, body = self.split(body, 'moneyline":"')
        moneyline, body = self.split(body, '","')
        _, body = self.split(body, 'overunder":')
        overunder, body = self.split(body, ',"')
        _, body = self.split(body, 'projected":')
        projected, body = self.split(body, ',"')
        _, body = self.split(body, 'projectedchange')
        _, body = self.split(body, 'value":')
        projected_change, body = self.split(body, '}')
        tmp = pd.DataFrame({
            'team': team,
            'oppo': oppo,
            'line': line,
            'moneyline': moneyline,
            'over_under': overunder,
            'projected_runs': projected,
            'projected_runs_change': projected_change
        }, index=[0])
        tmp['today'] = dt.now().date()
        return self.parse_js(body, df.append(tmp).reset_index(drop=True))
