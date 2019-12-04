from five_thirty_eight.utilities import FiveThirtyEightDataset
from doltpy.etl import get_df_table_writer, get_dolt_loader
from datetime import datetime

SUBPATH = 'soccer-api'
DATASETS = {'spi_global_rankings_intl': ('international', ['name', 'confed']),
            'spi_global_rankings': ('club', ['name', 'league']),
            'spi_matches': ('club', ['date', 'league_id', 'team1', 'team2'])}
ELO_DATASETS = [FiveThirtyEightDataset('{}/{}'.format(SUBPATH, infix), name, primary_keys)
                for name, (infix, primary_keys) in DATASETS.items()]


def get_loaders():
    loaders = [get_df_table_writer(elo_dataset.name, elo_dataset.get_dataset_fetcher(), elo_dataset.primary_keys)
               for elo_dataset in ELO_DATASETS]
    return [get_dolt_loader(loaders, True, 'Updated NBA ELO data for {}'.format(datetime.now()))]