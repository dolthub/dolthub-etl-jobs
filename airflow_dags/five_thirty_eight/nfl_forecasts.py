from five_thirty_eight.utilities import FiveThirtyEightDataset
from doltpy.etl import get_df_table_writer, get_dolt_loader
from datetime import datetime

SUBPATH = 'nfl-api'
PRIMARY_KEYS = ['date', 'season', 'team1', 'team2']
ELO_DATASETS = [FiveThirtyEightDataset(SUBPATH, 'nfl_elo', PRIMARY_KEYS)]


def get_loaders():
    loaders = [get_df_table_writer(elo_dataset.name, elo_dataset.get_dataset_fetcher(), elo_dataset.primary_keys)
               for elo_dataset in ELO_DATASETS]
    return [get_dolt_loader(loaders, True, 'Updated NFL ELO data for {}'.format(datetime.now()))]
