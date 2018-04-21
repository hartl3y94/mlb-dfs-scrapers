import time
import random
import logging

from scrapers.fangraphs import FanGraphsScraper
from scrapers.rotoguru import RotoGuruScraper
from scrapers.statcast import StatcastScraper
from scrapers.weather import WeatherScraper
from scrapers.vegas import VegasScraper
from util.config import get_config

def scrape_fangraphs(table_cfg):
    """ Run the fangraphs scraper against the table set """
    FANGRAPHS_TABLES = table_cfg['fangraphs']
    scraper = FanGraphsScraper()

    for table, info in FANGRAPHS_TABLES.items():
        scraper.fetch(
            url=info['url'],
            js_cmd=info['js_cmd'],
            filename=info['filename'],
            column_list=info['columns'],
            table_name=table
        )
        time.sleep(random.randint(2, 8))

def scrape_rotoguru(table_cfg):
    """ Run rotoguru scraper using account login """
    ROTO_TABLES = table_cfg['rotoguru']
    scraper = RotoGuruScraper()

    LOGIN = get_config('accounts.yml')['rotoguru']

    for table, info in ROTO_TABLES.items():
        scraper.fetch(
            url=info['url'] % (LOGIN['username'], LOGIN['password']),
            column_list=info['columns'],
            table_name=table
        )

def scrape_statcast(table_cfg):
    """ Run statcast batters scraper """
    STAT_TABLES = table_cfg['statcast']
    scraper = StatcastScraper()

    for table, info in STAT_TABLES.items():
        scraper.fetch(
            url=info['url'],
            column_list=info['columns'],
            table_name=table
        )

def scrape_weather(table_cfg):
    """ Run weather scrapers """
    WEATHER_TABLES = table_cfg['weather']
    scraper = WeatherScraper()

    for table, info in WEATHER_TABLES.items():
        scraper.fetch(
            url=info['url'],
            table_name=table
        )

def scrape_vegas(table_cfg):
    """ Run vegas line scrapers """
    VEGAS_TABLES = table_cfg['vegas']
    scraper = VegasScraper()

    for table, info in VEGAS_TABLES.items():
        scraper.fetch(
            url=info['url'],
            table_name=table
        )

if __name__ == '__main__':
    # Configure logging
    FORMAT = '[%(levelname)s %(asctime)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    # Load in table config
    TABLE_CFG = get_config('tables.yml')

    # Fangraphs
    scrape_fangraphs(TABLE_CFG)

    # Rotoguru
    scrape_rotoguru(TABLE_CFG)

    # Statcast
    scrape_statcast(TABLE_CFG)

    # Weather
    scrape_weather(TABLE_CFG)

    # Vegas
    scrape_vegas(TABLE_CFG)
