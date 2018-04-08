import logging

from scrapers.fangraphs import FanGraphsScraper

FANGRAPHS_TABLES = {
    'fg_batters': {
        'url': 'http://www.fangraphs.com/leaders.aspx?pos=all&stats=bat&lg=all&qual=0&type=c,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,60,61,62,102,103,104,105,106,107,108,109,110,111,206,207,208,209,210,211&season=2018&month=0&season1=2018&ind=0&team=&rost=&age=&filter=&players=',
        'columns': ['name','team','age','g','ab','pa','h','one_b','two_b','three_b',
            'hr','r','rbi','bb','ibb','so','hbp','sf','sh','gdp','sb','cs','avg','ifh','bu',
            'buh','bb_perc','k_perc','bb_k','obp','slg','ops','iso','babip','gb_fb','ld_perc',
            'gb_perc','fb_perc','iffb_perc','hr_fb','ifh_perc','buh_perc','woba','wraa','wrc',
            'spd','wrc_plus','wpa','o_swing_perc','z_swing_perc','swing_perc','o_contact_perc',
            'z_contact_perc','contact_perc','zone_perc','f_strike_perc','swstr_perc','bsr',
            'pull_perc','cent_perc','oppo_perc','soft_perc','med_perc','hard_perc','fg_id']
    },
}


if __name__ == '__main__':
    # Configure logging
    FORMAT = '[%(levelname)s %(asctime)s] %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    # Fangraphs
    scraper = FanGraphsScraper()

    for table, info in FANGRAPHS_TABLES.items():
        scraper.fetch(
            url=info['url'],
            column_list=info['columns'],
            table_name=table)

