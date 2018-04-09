import time
import logging

from scrapers.fangraphs import FanGraphsScraper
from util.config import get_config

def scrape_fangraphs(table_cfg):
    """ Run the fangraphs scraper against the table set """
    FANGRAPHS_TABLES = table_cfg['fangraphs']
    scraper = FanGraphsScraper()

    count = 0
    for table, info in FANGRAPHS_TABLES.items():
        count += 1
        if count == 1:
            continue

        scraper.fetch(
            url=info['url'],
            js_cmd=info['js_cmd'],
            filename=info['filename'],
            column_list=info['columns'],
            table_name=table
        )
        time.sleep(5)



if __name__ == '__main__':
    # Configure logging
    FORMAT = '[%(levelname)s %(asctime)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    # Load in table config
    TABLE_CFG = get_config('tables.yml')

    # Fangraphs
    scrape_fangraphs(TABLE_CFG)
