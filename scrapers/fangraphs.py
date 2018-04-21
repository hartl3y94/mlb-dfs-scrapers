import os
import sys
import time
import logging

import pandas as pd
from pyvirtualdisplay import Display
from selenium import webdriver

from scrapers.base import BaseScraper

# Download timeout limit
TIMEOUT = 600

class FanGraphsScraper(BaseScraper):
    """ This class utilizes the selenium webdriver
        along with chromium webdriver to directly
        download data from javascript calls
    """

    def __init__(self):
        # Default filename upon downloading a file
        super(FanGraphsScraper, self).__init__()
        self.create_display()

    @staticmethod
    def create_display():
        """ Create virual display to feed into selenium
        """
        display = Display(visible=0, size=(800, 600))
        display.start()

    @staticmethod
    def create_driver(download_path, log_path, adblock_path=None):
        """ Create chrome webdriver specifying target directory
            for file downloads, logging directory, and optionally
            specifying path to an Adblock extension
        """
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": os.path.dirname(download_path),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        if adblock_path:
            options.add_extension(adblock_path)
        return webdriver.Chrome(
            chrome_options=options,
            service_args=['--log-path=%s' % log_path])

    def fetch(self, url, js_cmd, filename, column_list, table_name):
        """ Download data from url by executing a javascript command
            by saving to local file via selenium. Load that file into
            memory, clean, then dump in s3 bucket
        """
        tmp_file = os.path.join(self.cfg['tmp_dir'], filename)
        self.validate_target(tmp_file)

        logging.info("Downloading %s to %s", table_name, tmp_file)

        # Create driver
        chrome_path = self.cfg['chrome_log_path']
        adblock_path = self.cfg['adblock_path']
        driver = self.create_driver(tmp_file, chrome_path, adblock_path)

        # Download and wait for file
        driver.get(url)
        driver.execute_script(js_cmd)

        tick = time.time()
        try:
            while True:
                time.sleep(1)
                if os.path.exists(tmp_file):
                    break
                elif (time.time() - tick) > TIMEOUT:
                    raise Exception("Download timed out")
        finally:
            driver.quit()

        # Read in downloaded data file
        df = pd.read_csv(tmp_file)
        df.columns = column_list
        os.remove(tmp_file)

        # Transfer to S3
        self.load_to_s3(df, table_name)
