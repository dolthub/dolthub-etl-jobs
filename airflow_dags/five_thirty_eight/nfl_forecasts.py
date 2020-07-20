from five_thirty_eight.utilities import FiveThirtyEightDataset, load_dataset

REPO_PATH = 'liquidata-demo-data/nfl-forecasts'
SUBPATH = 'nfl-api'
PRIMARY_KEYS = ['date', 'season', 'team1', 'team2']
ELO_DATASETS = [FiveThirtyEightDataset(SUBPATH, 'nfl_elo', PRIMARY_KEYS)]

if __name__ == 'main':
    load_dataset()
