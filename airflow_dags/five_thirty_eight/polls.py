from five_thirty_eight.utilities import FiveThirtyEightDataset
from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub
from datetime import datetime

REPO_PATH = 'liquidata-demo-data/polls'

BASE_PRIMARY_KEYS = ['question_id', 'poll_id', 'pollster_id', 'sponsor_ids']
POLLS = {'president_primary_polls': BASE_PRIMARY_KEYS + ['candidate_id'],
         'president_polls': BASE_PRIMARY_KEYS + ['candidate_name'],
         'senate_polls': BASE_PRIMARY_KEYS + ['candidate_name'],
         'house_polls': BASE_PRIMARY_KEYS + ['candidate_name'],
         'governor_polls': BASE_PRIMARY_KEYS + ['candidate_name'],
         'president_approval_polls': BASE_PRIMARY_KEYS,
         'generic_ballot_polls': BASE_PRIMARY_KEYS}
SUBPATH = 'polls-page'
DATASETS = [FiveThirtyEightDataset(SUBPATH, name, pks) for name, pks in POLLS.items()]


def load():
    table_writers = [get_df_table_writer(poll.name, poll.get_dataset_fetcher(), poll.primary_keys) for poll in DATASETS]
    loaders = [get_dolt_loader(table_writers, True, 'Updated poll data {}'.format(datetime.now()))]
    load_to_dolthub(loaders, clone=True, push=True, remote_name='origin', remote_url=REPO_PATH)


if __name__ == 'main':
    load()
