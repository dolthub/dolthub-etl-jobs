#!/usr/local/bin/python3

import pandas as pd 

from doltpy.core import Dolt

from pprint import pprint

url_base = 'https://raw.githubusercontent.com/guga31bb/nflfastR-data/master/data/'

repo = Dolt('.')

pbp_df = pd.DataFrame()
year   = 2000
while year < 2020:  
    url = url_base + 'play_by_play_' + str(year) + '.csv.gz?raw=True'
    i_data = pd.read_csv(url, compression='gzip', low_memory=False)
    pbp_df = pbp_df.append(i_data, sort=True)

    year += 1

#Give each row a unique index
pbp_df.reset_index(drop=True, inplace=True)
    
plays_pks = ['game_id', 'play_id']
plays_csv_file = 'plays.csv'

repo.import_df('plays', pbp_df, plays_pks, 'update')
