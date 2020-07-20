from typing import List
import requests
import pandas as pd
from io import BytesIO
import logging
from datetime import datetime
from doltpy.etl import get_df_table_writer, get_dolt_loader, load_to_dolthub

logger = logging.getLogger(__name__)
BASE_URL = 'https://projects.fivethirtyeight.com'


class FiveThirtyEightDataset:

    def __init__(self, subpath: str, name: str, primary_keys: List[str]):
        self.name = name
        self.table_name = self.name
        self.primary_keys = primary_keys
        self.subpath = subpath

    def get_data_url(self):
        return '{}/{}/{}.csv'.format(BASE_URL, self.subpath, self.name)

    def get_dataset_fetcher(self):
        def inner():
            logger.info('Fetching data from {}'.format(self.get_data_url()))
            r = requests.get(self.get_data_url())
            if r.status_code == 200:
                return pd.read_csv(BytesIO(r.content))
            else:
                raise ValueError('Got status code {}'.format(r.status_code))

        return inner


def load_dataset(repo_path: str, datasets: List[FiveThirtyEightDataset], message: str):
    table_writers = [get_df_table_writer(ds.name, ds.get_dataset_fetcher(), ds.primary_keys) for ds in datasets]
    loaders = [get_dolt_loader(table_writers, True, message)]
    load_to_dolthub(loaders, clone=True, push=True, remote_name='origin', remote_url=repo_path)
