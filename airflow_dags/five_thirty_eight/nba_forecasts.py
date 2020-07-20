from five_thirty_eight.utilities import FiveThirtyEightDataset, load_dataset

SUBPATH = 'nba-model'
PRIMARY_KEYS = ['date', 'season', 'team1', 'team2']
ELO_DATASETS = [FiveThirtyEightDataset(SUBPATH, 'nba_elo', PRIMARY_KEYS)]
REPO_PATH = 'liquidata-demo-data/nba-forecasts'

if __name__ == 'main':
    load_dataset(REPO_PATH, ELO_DATASETS, '')
