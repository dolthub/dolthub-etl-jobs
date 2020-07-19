from five_thirty_eight.utilities import FiveThirtyEightDataset
from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub
from datetime import datetime


REPO_PATH = 'liquidata-demo-data/nfl-forecasts'


SUBPATH = 'nfl-api'
PRIMARY_KEYS = ['date', 'season', 'team1', 'team2']
ELO_DATASETS = [FiveThirtyEightDataset(SUBPATH, 'nfl_elo', PRIMARY_KEYS)]


def load():
    table_writers = [get_df_table_writer(elo_dataset.name, elo_dataset.get_dataset_fetcher(), elo_dataset.primary_keys)
                     for elo_dataset in ELO_DATASETS]
    loaders = [get_dolt_loader(table_writers, True, 'Updated NFL ELO data for {}'.format(datetime.now()))]
    load_to_dolthub(loaders, clone=True, push=True, remote_name='origin', remote_url=REPO_PATH)


if __name__ == 'main':
    load()
