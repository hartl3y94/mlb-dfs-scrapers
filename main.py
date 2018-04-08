import logging

from scrapers.fangraphs import FanGraphsScraper
from util.config import get_config


if __name__ == '__main__':
    # Configure logging
    FORMAT = '[%(levelname)s %(asctime)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    # Load in table config
    TABLE_CFG = get_config('tables.yml')

    # Fangraphs
    scraper = FanGraphsScraper()
    FANGRAPHS_TABLES = TABLE_CFG['fangraphs']

    for table, info in FANGRAPHS_TABLES.items():
        scraper.fetch(
            url=info['url'],
            column_list=info['columns'],
            table_name=table)

