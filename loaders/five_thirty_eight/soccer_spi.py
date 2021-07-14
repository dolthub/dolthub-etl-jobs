#!/usr/local/bin/python3

from five_thirty_eight.utilities import FiveThirtyEightDataset, load_dataset

REPO_PATH = 'liquidata-demo-data/soccer-spi'
SUBPATH = 'soccer-api'
DATASETS = {'spi_global_rankings_intl': ('international', ['name', 'confed']),
            'spi_global_rankings': ('club', ['name', 'league']),
            'spi_matches': ('club', ['date', 'league_id', 'team1', 'team2'])}
ELO_DATASETS = [FiveThirtyEightDataset('{}/{}'.format(SUBPATH, infix), name, primary_keys)
                for name, (infix, primary_keys) in DATASETS.items()]

if __name__ == 'main':
    load_dataset(REPO_PATH, ELO_DATASETS, '')