#!/usr/local/bin/python3

from doltpy.core import Dolt

import pandas
import random
import time
import os
import csv

from pprint import pprint

from nba_api.stats.static import teams
from nba_api.stats.static import players

from nba_api.stats.endpoints import playercareerstats

table_map = {
    'CareerTotalsAllStarSeason': 'career_totals_allstar',
    'CareerTotalsPostSeason': 'career_totals_post_season',
    'CareerTotalsRegularSeason': 'career_totals_regular_season',
    'SeasonRankingsPostSeason': 'season_rankings_post_season',
    'SeasonRankingsRegularSeason': 'season_rankings_regular_season',
    'SeasonTotalsAllStarSeason': 'season_totals_allstar',
    'SeasonTotalsPostSeason': 'season_totals_post_season',
    'SeasonTotalsRegularSeason': 'season_totals_regular_season'
    }

players_df = pandas.DataFrame(players.get_players())

count = 1
total = len(players_df.index)
for player_id in players_df['id']:
    print(f'{count}/{total}: {player_id}')

    dirpath = f'player-data/{player_id}'
    if not os.path.isdir(dirpath):
        try:
            os.mkdir(dirpath)
        except OSError:
            print ("Creation of the directory %s failed" % dirpath)
    
    career = playercareerstats.PlayerCareerStats(player_id=player_id)
    stats_dicts = career.get_dict()

    write_header = 1;
    for stats_dict in stats_dicts['resultSets']:
        table_name = stats_dict['name']                                       
        filepath = f'player-data/{player_id}/{table_name}.csv'

        columns = [x.lower() for x in stats_dict['headers']]
        if write_header:
            with open(filepath, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(columns)
                write_header = 0

        
        with open(filepath, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            for row in stats_dict['rowSet']:
                cleanrow = []
                for stat in row:
                    if isinstance(stat, str) and stat == 'NR':
                        stat = ''
                    cleanrow.append(stat)
                        
                csvwriter.writerow(cleanrow)
                    
        write_header = 1
    
    count += 1
    time.sleep(random.random()*30)
