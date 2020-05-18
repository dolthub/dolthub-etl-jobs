#!/usr/local/bin/python3

import pandas as pd 

from doltpy.core import Dolt

url_base = 'https://raw.githubusercontent.com/guga31bb/nflfastR-data/master/'
roster_path = 'roster-data/roster.csv.gz'

repo = Dolt('.')

# Start with rosters CSV
rosters_url = url_base + roster_path
rosters_df  = pd.read_csv(rosters_url,
                          compression='gzip',
                          low_memory=False)

# Grab the teams table from rosters dataframe
filter_col = [col for col in rosters_df if col.startswith('team.')]

teams_df = rosters_df[filter_col]
teams_df = teams_df.drop_duplicates()
teams_df = teams_df.rename(columns=lambda x: x.replace('team.', ''))

teams_csv_file = 'teams.csv'
teams_pks = ['season', 'teamId']

repo.import_df('teams', teams_df, teams_pks, 'update')

# Grab the players table from the rosters dataframe
filter_col = [col for col in rosters_df if col.startswith('teamPlayers.')]
filter_col = filter_col + ['team.season', 'team.teamId']

players_df = rosters_df[filter_col]
players_df = players_df.rename(columns=lambda x: x.replace('team.', ''))
players_df = players_df.rename(columns=lambda x: x.replace('teamPlayers.', ''))

players_df['jerseyNumber'] = players_df['jerseyNumber'].fillna('')
players_df['jerseyNumber'] = players_df['jerseyNumber'].astype(str)
players_df['jerseyNumber'] = players_df['jerseyNumber'].str.split('.')
players_df['jerseyNumber'] = players_df['jerseyNumber'].str[0]
players_df['birthDate'] = pd.to_datetime(players_df['birthDate'])

players_csv_file = 'players.csv'
players_pks = ['season', 'teamId', 'nflId']
repo.import_df('players', players_df, players_pks, 'update')
    
