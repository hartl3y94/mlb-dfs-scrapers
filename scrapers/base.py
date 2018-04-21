import abc
import os
import logging
from io import StringIO
import boto3

from util.config import get_config

class BaseScraper(object):
    """ Abstract class for scraper object used to fetching
        data from website and dumping into an s3 bucket
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        """ Default just initialize with the config file
        """
        self.cfg = get_config()

    @abc.abstractmethod
    def fetch(self, **kwargs):
        """ This method should fetch data and format as a
            pandas.DataFrame, and end with a call to the
            self.load_to_s3 method
        """
        pass

    @staticmethod
    def validate_target(file_path):
        """ Shared static method for verifying a target's
            directory exists and remove existing file
            if it is present
        """
        FULL_PATH = os.path.realpath(file_path)
        if not os.path.exists(os.path.dirname(FULL_PATH)):
            os.makedirs(os.path.dirname(FULL_PATH))
        if os.path.exists(FULL_PATH):
            os.remove(FULL_PATH)

    def load_to_s3(self, df, table_name):
        """ Write a dataframe to s3 bucket as a csv file
        """
        # Write to csv in memory buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Create s3 interface from config
        BUCKET = self.cfg['s3']['bucket']
        DIR = self.cfg['s3']['data_dir']

        # This line can be changed to include timestamps
        # in the file name if you wish to store file versions
        # using a hive structure. For now overwriting one file is fine
        target_file = os.path.join(DIR, table_name, table_name + '.csv')

        # Load
        logging.info("Loading to S3 bucket %s/%s", bucket, target_file)

        s3 = boto3.resource('s3')
        s3.Object(bucket, target_file).put(Body=csv_buffer.getvalue())
