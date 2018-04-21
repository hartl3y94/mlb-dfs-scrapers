import logging
from urllib.request import urlopen

import pandas as pd

from scrapers.base import BaseScraper

class StatcastScraper(BaseScraper):
	""" This class uses urllib request to parse
		data directly from a json-like variable
		in javascript page to pandas and finally s3
	"""

	def fetch(self, url, column_list, table_name):
		""" Download data from webpage source json page
			variable and dump in s3 as csv
		"""
		logging.info("Downloading %s from Statcast", table_name)

		# GET -> string
		text = urlopen(url).read().decode('latin1')
		a = text.find("var leaderboard_data = [") + 25
		text = text[a:]
		a = text.find("}];")
		text = text[:a]
		text = text.replace('%','')
		text = text.replace('null', '"null"')

		# String -> list[list[str]]
		data = text.split("},{")
		data = [row.split(':')[1:] for row in data]
		data = [list(map(lambda value: value.split('","')[0].replace('"', ''), row)) for row in data]

		# list[list[str]] -> dataframe
		df = pd.DataFrame(data)
		df.columns = column_list

		# -> s3
		self.load_to_s3(df, table_name)