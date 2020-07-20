#!/usr/local/bin/python3

from five_thirty_eight.utilities import FiveThirtyEightDataset, load_dataset

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

if __name__ == 'main':
    load_dataset(REPO_PATH, DATASETS, '')
