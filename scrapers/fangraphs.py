import os
import sys
import time
import logging
from io import StringIO

import boto3
import pandas as pd
from pyvirtualdisplay import Display
from selenium import webdriver

from util.config import get_config

# Download timeout limit
TIMEOUT = 600


class FanGraphsScraper(object):
    """ This class utilizes the selenium webdriver
        along with chromium webdriver to directly
        download data from javascript calls
    """

    def __init__(self, **kwargs):
        # Default filename upon downloading a file
        self.cfg = get_config()
        self.create_display()

    @staticmethod
    def validate_target(file_path):
        """ Verify the target directory exists, and
            remove existing file if it exists
        """
        FULL_PATH = os.path.realpath(file_path)
        if not os.path.exists(os.path.dirname(FULL_PATH)):
            os.makedirs(os.path.dirname(FULL_PATH))
        if os.path.exists(FULL_PATH):
            os.remove(FULL_PATH)

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
        """ Main execution. Download data from url, parse
            into .CSV then export to S3
        """
        tmp_file = os.path.join(self.cfg['tmp_dir'], table_name, filename)
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

    def load_to_s3(self, df, table_name):
        """ Load a DataFrame to S3 bucket as an S3 file
        """
        # Write to csv in memory buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Create S3 interface
        bucket = self.cfg['s3']['bucket']
        target_file = os.path.join(self.cfg['s3']['data'], table_name, table_name + '.csv')

        # Load
        logging.info("Loading to S3 bucket %s/%s", bucket, target_file)

        s3 = boto3.resource('s3')
        s3.Object(bucket, target_file).put(Body=csv_buffer.getvalue())
