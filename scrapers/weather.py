import logging
from urllib.request import urlopen
from datetime import datetime as dt

import pandas as pd
import numpy as np

from scrapers.base import BaseScraper

class WeatherScraper(BaseScraper):
    """ This class ingests a fairly complicated HTML page
        of weather data and converts it into a usable pandas
        dataframe format and loads to s3
    """

    def fetch(self, url, table_name):
        """ Download webpage HTML and parse into simple
            dataframe format
        """
        logging.info("Downloading %s from %s", table_name, url)

        # url -> string
        body = urlopen(url).read().decode('latin1')

        # string -> dataframe
        df = parse_weather(body)

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

    @staticmethod
    def make_list(txt, start, end=''):
        """ Make a list from HTML table row """
        _, txt = self.split(txt, start)
        _, txt = self.split(txt, '</td>')
        tmp_list = []
        for i in range(9):
            _, txt = self.split(txt, '>')
            t, txt = self.split(txt, '%s</td>' % end)
            tmp_list.append(t)
        return tmp_list, txt

    @staticmethod
    def str2int(x):
        """ Parse int value """
        a = x.find(' ')
        return int(x[:a])

    @staticmethod
    def parse_dome(tmp):
        """ Parse row stats if field has a dome """
        tmp['precip_perc'] = 0
        tmp['w_speed'] = 0
        tmp['w_dir'] = 'N/A'
        tmp['w_condition'] = 'Roof'
        return tmp

    @staticmethod
    def parse_weather(text, df=pd.DataFrame()):
        """ Recursivley parse HTML table into dataframe """
        if text.find('target="_blank" class="weather">') == -1:
            return df
        col_names = ['team','today','game_time','adi','temp','humidity','feels_like',
                     'w_condition','precip_perc','w_speed','w_dir']  
        _, text = self.split(text, 'target="_blank" class="weather">')
        _, text = self.split(text, ' at ')
        team, text = self.split(text, '\x96')
        time, text = self.split(text, '-')

        has_roof = False
        is_dome = False
        a = text.find('may neutralize some weather')
        if (a < 500) & (a > -1):
            has_roof = True
        a = text.find('weather details are not relevant')
        if (a < 500) & (a > -1):
            is_dome = True

        if not is_dome:
            _, text = self.split(text, 'Wind: <br>')
            w_speed, text = self.split(text, '<br></td>')

            _, text = self.split(text, '/weather/wind/')
            _, text = self.split(text, '/')
            w_dir, text = self.split(text, '.gif')

            times, text = self.make_list(text, 'Time:')
            temps, text = self.make_list(text, 'Temp:', '&deg;')
            humid, text = self.make_list(text, 'Humidity:', '%')
            feels, text = self.make_list(text, 'Feels like:', '&deg;')
            conds, text = self.make_list(text, 'Condition:')
            precp, text = self.make_list(text, 'Precip%:', '%')
            winds, text = self.make_list(text, 'Wind:')

            a = time.find(':')
            game_time = int(time[:a])
            times = list(map(self.str2int, times))
            time_match = np.where([x == game_time for x in times])[0]
            if len(time_match) != 0:
                idx = time_match[0]
            else:
                idx = 0

            tmp = pd.DataFrame({'temp':temps[idx], 'humidity':humid[idx], 'feels_like':feels[idx],
                                   'w_condition':conds[idx], 'precip_perc':precp[idx]}, index=[0])
        else:
            tmp = pd.DataFrame({'temp':80, 'humidity':50, 'feels_like':80, 
                                    'w_condition':'Dome', 'precip_perc':0}, index=[0])

        game_time, zone = self.split(time, ' ')
        game_time = float(game_time.replace(':','.'))
        if int(game_time) != 12:
            game_time += 12
        if zone == 'CDT':
            game_time += 1
        elif zone == 'MDT':
            game_time += 2
        elif zone == 'PDT':
            game_time += 3

        tmp['team'] = team
        tmp['today'] = dt.strftime(dt.now(), '%Y-%m-%d')
        tmp['game_time'] = game_time

        if not is_dome:
            _, w_speed = self.split(w_speed, ' ')
            w_speed, _ = self.split(w_speed, ' mph')

            w_dir_dict = {'s':'Out to CF', 'se':'Out to LF', 'sw':'Out to RF',
                          'n':'In from CF', 'ne':'In from RF', 'nw':'In from LF',
                          'w':'L to R', 'e':'R to L'}

            if len(w_dir) == 3:
                w_dir = w_dir[1:]
            w_dir = w_dir_dict[w_dir]

            tmp['adi'] = np.nan
            tmp['w_speed'] = int(w_speed)
            tmp['w_dir'] = w_dir
        else:
            tmp['adi'] = np.nan
            tmp['w_speed'] = 0
            tmp['w_dir'] = 'N/A'

        tmp = tmp[col_names]
        if team == 'San Francisco Giants':
            has_roof = True
        if has_roof:
            tmp = self.parse_dome(tmp)

        df = df.append(tmp)
        return self.parse_weather(text, df)